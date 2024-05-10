from dukeauth import get_auth
from dukeoutages import main

headers, cookies = get_auth()

if __name__ == '__main__':
    try:
        main(headers, cookies)
    except ValueError as e:
        print(e)
    except Exception as e:
        print(e)
