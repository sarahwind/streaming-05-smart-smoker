"""
BBQ producer

Author: Sarah Windeknecht
Date: February 14, 2023
"""

import csv
import pika
import sys
import time
import webbrowser

# declare variables
smoker_temp_queue = "01-smoker"
foodA_temp_queue = "02-food-A"
foodB_temp_queue = "03-food-B"
input_file = open("smoker-temps.csv", "r")

def offer_rabbitmq_admin_site(show_offer):

    """Offer to open the RabbitMQ Admin website"""

    if show_offer == "True":
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()
    else: 
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
            ch = conn.channel()
        # clear queue to clear out old messages
            ch.queue_delete(queue=queue_name)
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
            ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
            ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
            print(f" [x] Sent {message} on {queue_name}")
    except pika.exceptions.AMQPConnectionError as e:
            print(f"Error: Connection to RabbitMQ server failed: {e}")
            sys.exit(1)
    finally:
        # close the connection to the server
            conn.close()

def message_from_csv(input_file):

     """ Opens the csv file, reads each row as a message, and sends the message to the queue. """

    # create a csv reader for our comma delimited data
     with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter=",")
        # skip header
        next(reader)
        for row in reader:
       
             # get timestamp column
            timestamp = f"{row[0]}"

             # convert numbers to floats
            try:
                smoker_temp = float(row[1])
                #get smoker temp and send message
                fstring_smoker_message = f"{timestamp}, {smoker_temp}"
                smoker_message = fstring_smoker_message.encode()
                send_message("localhost", smoker_temp_queue, smoker_message)
            except ValueError:
                pass

            try:
                foodA_temp = float(row[2])
                # get food A temp and send message
                fstring_foodA_message = f"{timestamp}, {foodA_temp}"
                foodA_message = fstring_foodA_message.encode()
                send_message("localhost", foodA_temp_queue, foodA_message)
            except ValueError:
                pass
    
            try:
                foodB_temp = float(row[3])
                # get food B and send message
                fstring_foodB_message = f"{timestamp}, {foodB_temp}"
                foodB_message = fstring_foodB_message.encode()
                send_message("localhost", foodB_temp_queue, foodB_message)
            except ValueError:
                pass
            
            # read values every half minute
            time.sleep(30)


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    
    offer_rabbitmq_admin_site("False")

    message_from_csv(input_file)

    

"""Guided Producer Design 
If this is the main program being executed (and you're not importing it for its functions),
We should call a function to ask the user if they want to see the RabbitMQ admin webpage.
We should call a function to begin the main work of the program.
As part of the main work, we should
Get a connection to RabbitMQ, and a channel, delete the 3 existing queues (we'll likely run this multiple times), and then declare them anew. 
Open the csv file for reading (with appropriate line endings in case of Windows) and create a csv reader.
For data_row in reader:
[0] first column is the timestamp - we'll include this with each of the 3 messages below
[1] Channel1 = Smoker Temp --> send to message queue "01-smoker"
[2] Channe2 = Food A Temp --> send to message queue "02-food-A"
[3] Channe3 = Food B Temp --> send to message queue "02-food-B"
Send a tuple of (timestamp, smoker temp) to the first queue
Send a tuple of (timestamp, food A temp) to the second queue
Send a tuple of (timestamp, food B temp) to the third queue 
Create a binary message from our tuples before using the channel to publish each of the 3 messages.
Messages are strings, so use float() to get a numeric value where needed
 Remember to use with to read the file, or close it when done.

 Producer Design Questions
Can the open() function fail?
What do we do if we know a statement can fail? Hint: try/except/finally
Does our data have header row? 
What happens if we try to call float("Channel1")? 
How will you handle the header row in your project?
Will you delete it (easy), or use code to skip it (better/more difficult)?

Producer Implementation Questions/Remarks
Will you use a file docstring at the top? Hint: yes
Where do imports go? Hint: right after the file/module docstring
After imports, declare any constants.
After constants, define functions.
Define a function to offer the RabbitMQ admin site, use variables to turn it off temporarily if desired.
Define a main function to
connect,
get a communication channel,
use the channel to queue_delete() all 3 queues 
use the channel to queue_declare() all 3 queues
open the file, get your csv reader, for each row, use the channel to basic_publish() a message
Use the Python idiom to only call  your functions if this is actually the program being executed (not imported). 
If this is the program that was called:
call your offer admin function() 
call your main() function, passing in just the host name as an argument (we don't know the queue name or message yet)
 

Handle User Interrupts Gracefully
Will this process be running for a while (half sec per record)?
If so, modify the code the option for the user to send a Keyboard interrupt (see earlier projects)"""