class ScriptReader(object):

    @staticmethod
    def get_script(path):
        return open(path, 'r').read()
        
        
class Messages(object):

    @staticmethod
    def print_message(msg):
        print('---> {}'.format(msg))