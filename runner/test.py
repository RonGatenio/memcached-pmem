import bmemcached
import argparse
import random
import string
import time

def parse_arguments():
    parser = argparse.ArgumentParser(description='Memcached Stress Test Script')
    parser.add_argument('-p', '--port', type=int, default=11211,
                        help='Port number of the Memcached server (default: 11211)')
    parser.add_argument('-n', '--keys', type=int, default=1000,
                        help='Number of keys to use in the stress test (default: 1000)')
    args = parser.parse_args()
    return args

def generate_random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for _ in range(length))

def stress_test(client, num_keys):
    keys = []
    start_time = time.time()

    print(f"Starting stress test with {num_keys} keys...")

    num_operations = num_keys * 3  # Total number of operations
    for i in range(num_operations):
        operation = random.choice(['set', 'get', 'delete'])
        
        if operation == 'set':
            key = generate_random_string(10)
            value = generate_random_string(50)
            client.set(key, value)
            keys.append((key, value))
        elif operation == 'get':
            if keys:
                key, expected_value = random.choice(keys)
                value = client.get(key)
                if value != expected_value:
                    print(f"Data mismatch for key {key}: expected {expected_value}, got {value}")
            else:
                # No keys to get, perform a set operation instead
                key = generate_random_string(10)
                value = generate_random_string(50)
                client.set(key, value)
                keys.append((key, value))
        elif operation == 'delete':
            if keys:
                index = random.randrange(len(keys))
                key, _ = keys.pop(index)
                client.delete(key)
            else:
                # No keys to delete, perform a set operation instead
                key = generate_random_string(10)
                value = generate_random_string(50)
                client.set(key, value)
                keys.append((key, value))
        else:
            # Should not reach here
            pass

        if (i + 1) % 1000 == 0:
            print(f"Performed {i + 1} operations...")

    end_time = time.time()
    duration = end_time - start_time

    print(f"Stress test completed in {duration:.2f} seconds.")

def run(port, keys):
    # Connect to the Memcached server
    client = bmemcached.Client((f'127.0.0.1:{port}', ))

    # Perform the stress test
    stress_test(client, keys)

    # Close the client connection
    client.disconnect_all()
        
def main():
    args = parse_arguments()
    run(args.port, args.keys)

if __name__ == '__main__':
    main()
