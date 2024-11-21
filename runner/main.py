import subprocess
import socket
import time
import os
import signal
import sys
import argparse

def get_full_path(*relative_paths):
    return os.path.join(os.path.dirname(__file__), *relative_paths)

def start_memcached(port, nthreads, pm_file, user):
    memcached_cmd = [
        get_full_path('..', 'memcached'),
        '-p', str(port),
        '-U', str(port),
        '-t', str(nthreads),
        '-A',
        '-o', f'pslab_force,pslab_file={pm_file},pslab_policy=pmem',
        '-u', user
    ]

    print("Starting Memcached with command:")
    print(' '.join(memcached_cmd))

    # Start Memcached as a subprocess
    #   Send stderr to the real stderr
    memcached_process = subprocess.Popen(memcached_cmd, stdout=subprocess.PIPE, stderr=None)

    return memcached_process

def wait_for_memcached(host, port, timeout=30):
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                print(f"Memcached is ready on {host}:{port}")
                return True
        except socket.error:
            time.sleep(0.2)
            if time.time() - start_time > timeout:
                print("Timed out waiting for Memcached to start.")
                return False

def run_client_script(port, num_keys):
    client_cmd = [
        sys.executable,  # This ensures we're using the same Python interpreter
        get_full_path('..', 'test.py'),
        '-p', str(port),
        '-n', str(num_keys)
    ]

    print("Running client script with command:")
    print(' '.join(client_cmd))

    client_process = subprocess.Popen(client_cmd)
    return client_process

def main():
    # Parse command-line arguments for main.py
    parser = argparse.ArgumentParser(description='Memcached Runner Script')
    parser.add_argument('-p', '--port', type=int, default=11211, help='Port number for Memcached')
    parser.add_argument('-t', '--nthreads', type=int, default=4, help='Number of threads for Memcached')
    parser.add_argument('-f', '--pm_file', type=str, default='/tmp/emihapwl_pmem_data', help='PMEM file path')
    parser.add_argument('-u', '--user', type=str, default='memcached-user', help='User to run Memcached')
    parser.add_argument('-n', '--num_keys', type=int, default=1000, help='Number of keys for the stress test')
    args = parser.parse_args()

    # Start Memcached
    memcached_process = start_memcached(args.port, args.nthreads, args.pm_file, args.user)

    # Wait for Memcached to be ready
    if not wait_for_memcached('127.0.0.1', args.port):
        print("Memcached did not start properly. Exiting.")
        memcached_process.terminate()
        sys.exit(1)

    # Run the client script
    client_process = run_client_script(args.port, args.num_keys)

    # Wait for the client script to finish
    client_process.wait()

    print("Client script has finished execution.")

    # Stop Memcached
    print("Stopping Memcached.")
    memcached_process.terminate()
    memcached_process.wait()
    print("Memcached has been stopped.")

if __name__ == '__main__':
    main()
