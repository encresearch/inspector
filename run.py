import logging

from inspector.inspector import main, datetime

if __name__ == '__main__':
    try:
        main()
    except Exception:
        msg = "The System Ecnountered an Exception on "
        date = datetime.now().strftime('%d %b %Y %H:%M')
        logging.basicConfig(filename='inspector.log')
        logging.exception("%s %s", msg, date)
