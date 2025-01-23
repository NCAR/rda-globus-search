#
# main function to run hello_world
#
def main():

   str = get_string('Hua')
   print(str)

def get_string(name):
   return name + ": Hello World!"

#
# call main() to start program
#
if __name__ == "__main__": main()
