import signal
import sys


def signal_handler(signum, frame):
    print "Script execution took too long. Interrupting"
    sys.exit()
    #raise Exception("end of time")
    
    
def loop_forever():
    import time
    while 1:
        print "sec"
        time.sleep(1)

if __name__ == '__main__':
    signal.signal(signal.SIGALRM, signal_handler)
    
    signal.alarm(7200)
    
    
    loop_forever()