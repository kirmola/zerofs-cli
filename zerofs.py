#!/usr/bin/env python3
"""
A command-line tool for uploading files to a ZeroFS.
Supports anonymous and authenticated uploads with multipart support.
"""

import argparse
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any, List
from base64 import b64encode
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
tqdm.set_lock(threading.RLock())


# Constants
MAX_RETRIES = 5
RETRY_BACKOFF_BASE = 2


class ProgressTracker:
    """Tracks and displays upload progress in real-time."""

    def __init__(self, total_size: int):
        self.total_size = total_size
        self.pbar = tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc="Uploading",
            ncols=100
        )

    def update(self, bytes_uploaded: int):
        """Update progress with newly uploaded bytes."""
        self.pbar.update(bytes_uploaded)

    def complete(self):
        """Mark progress as complete."""
        self.pbar.close()


class ClientAuth:
    """Handles authentication token loading."""

    def __init__(self, token: Optional[str] = None, token_file: str = 'auth.json'):
        self.token = token
        self.token_file = token_file

    def get_token(self) -> Optional[str]:
        """Retrieve authentication token from CLI arg or file."""
        if self.token:
            return self.token

        # Try loading from file
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                    return data.get('token')
            except (json.JSONDecodeError, IOError) as e:
                print(
                    f'Warning: Could not load token from {self.token_file}: {e}')

        return None


class APIClient:
    """Handles communication with application upload endpoints."""

    def __init__(self, api_base_url: str):
        self.api_base_url = api_base_url.rstrip('/')
        self.session = self._create_session()

    def _create_session(self):
        """Create requests session with retry logic."""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def initiate_upload(self, filename: str, file_size: int, bucket_code: str,
                        content_type: str = 'application/octet-stream',
                        note: str = '', token: Optional[str] = None) -> Dict[str, Any]:
        """
        Initiate upload and get presigned URLs from API server.
        Server manages SSE-C encryption and returns presigned URLs.
        """
        payload = {
            'filename': filename,
            'file_size': file_size,
            'bucket_code': bucket_code,
            'content_type': content_type,
            'note': note,
            'token': token
        }

        try:
            response = self.session.post(
                f'{self.api_base_url}/initiate-upload/',
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(f'Failed to initiate upload: {e}')

    def complete_multipart_upload(self, completion_token: str, parts: List[Dict],
                                  token: Optional[str] = None) -> Dict[str, Any]:
        """Notify API server to complete multipart upload."""
        payload = {
            'completion_token': completion_token,
            'parts': parts,
            'token': token

        }

        try:
            response = self.session.post(
                f'{self.api_base_url}/complete-multipart-upload/',
                json=payload,
                timeout=60
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(f'Failed to complete multipart upload: {e}')

    def abort_multipart_upload(self, completion_token: str,
                               token: Optional[str] = None):
        """Notify API server to abort multipart upload."""
        payload = {
            'completion_token': completion_token,

        }

        try:
            response = self.session.post(
                f'{self.api_base_url}/abort-multipart-upload/',
                json=payload,
                timeout=30
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f'Warning: Failed to abort multipart upload: {e}')

    def complete_single_upload(self, completion_token: str,
                               token: Optional[str] = None) -> Dict[str, Any]:
        """Mark single upload as complete in database."""
        payload = {
            'completion_token': completion_token,
            'token': token
        }

        try:
            response = self.session.post(
                f'{self.api_base_url}/complete-single-upload/',
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(f'Failed to complete single upload: {e}')


class Uploader:
    """Handles file uploads directly to storage using presigned URLs."""

    def __init__(self, api_client: APIClient):
        self.api_client = api_client
        self.session = self._create_session()

    def _create_session(self):
        """Create requests session for storage uploads."""
        session = requests.Session()
        return session

    def upload_file(self, file_path: str, upload_metadata: Dict[str, Any],
                    token: Optional[str] = None) -> bool:
        """
        Upload file directly to storage using presigned URLs.
        Automatically handles single or multipart upload based on metadata.
        """
        upload_type = upload_metadata.get('upload_type')

        if upload_type == 'single':
            return self._single_upload(file_path, upload_metadata, token)
        elif upload_type == 'multipart':
            return self._multipart_upload(file_path, upload_metadata, token)
        else:
            raise ValueError(f'Unknown upload type: {upload_type}')

    def _single_upload(self, file_path: str, metadata: Dict[str, Any],
                       token: Optional[str] = None) -> bool:
        """Perform single file upload directly to storage."""
        print(f'Starting single upload for {os.path.basename(file_path)}...')
        file_size = os.path.getsize(file_path)
        progress = ProgressTracker(file_size)

        try:
            url = metadata['url']
            headers = metadata.get('headers', {})

            with open(file_path, 'rb') as f:
                response = self.session.put(
                    url,
                    data=f,
                    headers=headers,
                    timeout=3600
                )
                response.raise_for_status()

            progress.complete()
            print('Upload to storage completed successfully!')

            result = self.api_client.complete_single_upload(
                metadata['completion_token'],
                token
            )

            metadata['file_id'] = result.get('file_id')

            print('Upload finalized.')
            return True

        except Exception as e:
            progress.complete()
            print(f'\nUpload failed: {e}')
            if hasattr(e, 'response') and e.response is not None:
                print(f'Response: {e.response.status_code} {e.response.text}')
            return False

    def _multipart_upload(self, file_path: str, metadata: Dict[str, Any],
                          token: Optional[str] = None) -> bool:
        """Perform multipart upload directly to storage using presigned URLs."""
        file_size = os.path.getsize(file_path)
        chunk_size = metadata['chunk_size']
        part_urls = metadata['part_urls']
        completion_token = metadata['completion_token']

        print(
            f'Starting multipart upload for {os.path.basename(file_path)}...')
        print(f'File size: {file_size / (1024**3):.2f} GB | '
              f'Chunk size: {chunk_size / (1024**2):.0f} MB | '
              f'Parts: {len(part_urls)}')

        progress = ProgressTracker(file_size)

        try:

            parts = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {}

                sse_c_headers = {
                    'x-amz-server-side-encryption-customer-algorithm': 'AES256',
                    'x-amz-server-side-encryption-customer-key': metadata.get('ssec_key'),
                    'x-amz-server-side-encryption-customer-key-md5': metadata.get('ssec_key_md5'),
                }

                for part_info in part_urls:
                    part_number = part_info['part_number']
                    part_url = part_info['url']

                    start = (part_number - 1) * chunk_size
                    end = min(start + chunk_size, file_size)

                    future = executor.submit(
                        self._upload_part,
                        file_path, part_url, part_number, start, end, progress, sse_c_headers
                    )
                    futures[future] = part_number

                for future in as_completed(futures):
                    part_number = futures[future]
                    try:
                        part_info = future.result()
                        parts.append(part_info)
                    except Exception as e:
                        print(f'\nPart {part_number} failed: {e}')
                        progress.complete()

                        self.api_client.abort_multipart_upload(
                            completion_token,
                            token
                        )
                        return False

            parts.sort(key=lambda x: x['part_number'])

            progress.complete()
            print('All parts uploaded to storage successfully!')

            print('Finalizing multipart upload with API server...')
            result = self.api_client.complete_multipart_upload(
                completion_token,
                parts,
                token
            )

            metadata['file_id'] = result.get('file_id')

            print('Multipart upload completed successfully!')
            return True

        except (Exception, KeyboardInterrupt) as e:
            progress.complete()
            print(f'\nMultipart upload failed: {e}')
            try:
                self.api_client.abort_multipart_upload(
                    completion_token,
                )
            except:
                pass
            return False

    def _upload_part(self, file_path: str, part_url: str, part_number: int,
                     start: int, end: int, progress: ProgressTracker,
                     sse_headers: Dict[str, str]) -> Dict[str, Any]:
        """Upload a single part with required SSE-C headers."""
        part_size = end - start

        for attempt in range(MAX_RETRIES):
            try:
                with open(file_path, 'rb') as f:
                    f.seek(start)
                    data = f.read(part_size)

                headers = {
                    'Content-Length': str(part_size),
                    **sse_headers
                }

                response = self.session.put(
                    part_url,
                    data=data,
                    headers=headers,
                    timeout=3600
                )
                response.raise_for_status()

                etag = response.headers.get('ETag', '').strip('"')
                progress.update(part_size)

                return {
                    'part_number': part_number,
                    'etag': etag
                }

            except requests.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_BACKOFF_BASE ** attempt
                    time.sleep(wait_time)
                    continue
                print(
                    f"Part {part_number} failed after {MAX_RETRIES} attempts: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(
                        f"Response: {e.response.status_code} {e.response.text}")
                raise


def main():
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description='Upload files to storage via API server (SSE-C encryption managed server-side)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:

  # Production (API at 'zerofs.link/api/')
  python uploader.py upload myfile.zip \\
    --bucket-code eu \\
    --api-url https://zerofs.link/api/ \\
    --token YOUR_TOKEN
        '''
    )

    subparsers = parser.add_subparsers(
        dest='command', help='Available commands')

    # Upload command
    upload_parser = subparsers.add_parser('upload', help='Upload a file')
    upload_parser.add_argument('file', help='Path to file to upload')
    upload_parser.add_argument('--bucket-code', required=True,
                               help='Bucket code (e.g., eu)')
    upload_parser.add_argument('--api-url', required=True,
                               help='API server base URL')
    upload_parser.add_argument('--note', default='',
                               help='Optional note/description for the file')
    upload_parser.add_argument('--content-type', default='application/octet-stream',
                               help='Content type (default: application/octet-stream)')
    upload_parser.add_argument('--token', help='Authentication token')
    upload_parser.add_argument('--token-file', default='auth.json',
                               help='Path to token file (default: auth.json)')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    if args.command == 'upload':
        # Validate file exists
        if not os.path.exists(args.file):
            print(f'Error: File not found: {args.file}')
            return 1

        file_path = os.path.abspath(args.file)
        file_size = os.path.getsize(file_path)
        filename = os.path.basename(file_path)

        print(f'File: {filename}')
        print(f'Size: {file_size / (1024**2):.2f} MB')
        print(f'Region Code: {args.bucket_code}')
        print(f'API Server: {args.api_url}')
        print()

        # Load authentication
        auth = ClientAuth(token=args.token, token_file=args.token_file)
        token = auth.get_token()

        if not token:
            print('Warning: No authentication token provided. Upload will be anonymous.')
            print()

        # Initialize API client (communicates with API)
        api_client = APIClient(api_base_url=args.api_url)

        print('Requesting presigned URLs from API server...')

        try:
            metadata = api_client.initiate_upload(
                filename=filename,
                file_size=file_size,
                bucket_code=args.bucket_code,
                content_type=args.content_type,
                note=b64encode(str(args.note).encode()).decode(),
                token=token
            )
        except RuntimeError as e:
            print(f'Error: {e}')
            return 1

        print(f'Upload type: {metadata.get("upload_type")}')
        print()

        uploader = Uploader(api_client)

        try:
            success = uploader.upload_file(file_path, metadata, token)
        except Exception as e:
            print(f'Error: {e}')
            return 1

        if not success:
            return 1

        # Display results
        print(f'\n✓ Upload successful!')

        main_domain = args.api_url.rstrip("/").rsplit("/", 1)[0]

        file_id = metadata.get('file_id')
        if file_id:
            print(f'File ID: {main_domain}/{file_id}/')

        download_url = metadata.get('download_url')
        if download_url:
            print(f'Download link: {download_url}')

        return 0


if __name__ == '__main__':
    sys.exit(main())
