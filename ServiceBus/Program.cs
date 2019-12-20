using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;

namespace ServiceBus
{

    class Program
    {
        // Connection string to queue
        const string ServiceBusConnectionString = "Endpoint=sb://ruexpsb.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=950mJ/AC/sxEULAMADHSnptDL1R6sok6ynP5OxvAenE=";
        const string QueueName = "ruexpq01";
        const string TopicName = "ruexpt01";
        
        static IQueueClient queueClient;
        static ITopicClient topicClient;
        static ISubscriptionClient subscriptionClient;

        static void Main(string[] args)
        {
            bool exit = false;
            while (!exit)
            {
            Console.WriteLine("======================================================");
            Console.WriteLine("SERVICE BUS SEND/RECEIVE CLIENT");
            Console.WriteLine("     1. SEND to Service Bus QUEUE");
            Console.WriteLine("     2. RECEIVE from Service Bus QUEUE");
            Console.WriteLine("     3. SEND to Service Bus TOPIC");
            Console.WriteLine("     4. RECEIVE from Service Bus SUBSCRIPTION");
            Console.WriteLine("     5. STATUS");
            Console.WriteLine("======================================================");
            Console.Write("Enter the action you want to perform:");
            int action = Convert.ToInt32(Console.ReadLine());
            
           switch(action)
            { 
                case 1:
                    // Send Messages to SB queue
                    SendtoqueueMainAsync().GetAwaiter().GetResult();
                    break;
                case 2:
                    // Receive Messages from SB queue
                    ReceiveFromQueueMainAsync().GetAwaiter().GetResult();
                    break;
                case 3:
                    // Send Messages to SB Topic
                    SendToTopicMainAsync().GetAwaiter().GetResult();
                    break;
                case 4:
                    // Receive Messages from SB Susbcription
                    ReceiveFromSubscriptionMainAsync().GetAwaiter().GetResult();
                    break;
                case 5:
                        // Receive Messages from SB Susbcription
                        CheckStatusMainAsync().GetAwaiter().GetResult();
                        break;
               default:
                    break;
            }
                Console.Write("Do you want to exit? y/n: ");
                if (Console.ReadLine() != "n")
                    exit = true;
            }

        }

        //SEND MESSAGES TO SERVICE BUS QUEUE
        static async Task SendtoqueueMainAsync()
        {
            // Adding this section to prompt for the number of messages to be sent
            Console.WriteLine("======================================================");
            Console.Write("Enter the number of messages you want to send to SB Queue:");
            

            int numberOfMessages = Convert.ToInt32(Console.ReadLine());

            // This line was used to send a fixed amount of messages. It is not needed because we are now prompting for the amount of messages to be sent.
            //const int numberOfMessages = 10;
            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

            //Console.WriteLine("=======================RUEXPSB========================");
            Console.WriteLine("======================================================");
            //Console.WriteLine("Press ENTER key to exit after sending all the messages");
            //Console.WriteLine("=========================SEND=========================");

            // Send Messages
            await SendMessagesToQueueAsync(numberOfMessages);


            //Console.ReadKey();

            await queueClient.CloseAsync();
                               
        }

        static async Task SendMessagesToQueueAsync(int numberOfMessagesToSend)
        {
            try
            {
                for (var i = 0; i < numberOfMessagesToSend; i++)
                {
                    // Create a new message to send to the queue
                    string messageBody = $"QueueMessage {i}";
                    var message = new Message(Encoding.UTF8.GetBytes(messageBody));

                    // Getting current time to add it to print it on screen
                    var time = DateTime.UtcNow;

                    //Write the body of the message to the console
                    Console.WriteLine($"Sending message: {messageBody} at {time}");

                    //Send the message to the queue
                    await queueClient.SendAsync(message);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        //SEND MESSAGES TO SERVICE BUS QUEUE

        //RECEIVE MESSAGES FROM SERVICE BUS QUEUE
        static async Task ReceiveFromQueueMainAsync()
        {
            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving messages in a loop");
         
            //Receive the queue message handler and receive messages in a loop
            RegisterOnMessageHandlerAndReceiveMessages();

            //Press ENTER key to exit after receiving messages in a loop
           Console.ReadKey();

            await queueClient.CloseAsync();
        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // configure the message handler options in term of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                //Maxim number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity
                //Set it according to how many messages the aplication wnats to process in parallel
                MaxConcurrentCalls = 1,

                // Indicates wheter the message pump should automatically complete the messages after returning from user callback.
                // False below indicates the complete operation is handled  by the user callback as in ProcessMessagesAsync().
                AutoComplete = false
            };

            // Register the function that processes messages
            queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            //Process the message
            //Added MessageID:{message.MessageId} and EnqueuedTimeUtc:{message.SystemProperties.EnqueuedTimeUtc}
            Console.WriteLine($"Receive message:  MessageID:{message.MessageId} SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)} EnqueuedTimeUtc:{message.SystemProperties.EnqueuedTimeUtc}");

            // Complete the message so that it is not received again
            // This can be done only if the queue Client is created in ReceiveMode.Peeklock mode (which is the default)
            await queueClient.CompleteAsync(message.SystemProperties.LockToken);

            //Note: Use the cancellationToken passed as necessary to determine if the queueClient ha already been closed
            //If queueClient has already been closed, you can choose to not call CompleteAsync() or AbanonAsync() etc.
        }

        // user this handler to examine the exceptions received on the message pump
        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Excetion context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
        //RECEIVE MESSAGES FROM SERVICE BUS QUEUE

        //SEND MESSAGES TO SERVICE BUS TOPIC
        static async Task SendToTopicMainAsync()
        {
            // Adding this section to prompt for the number of messages to be sent
            Console.WriteLine("======================================================");
            Console.Write("Enter the number of messages you want to send to SB Topic:");

            int numberOfMessages = Convert.ToInt32(Console.ReadLine());

            // This line was used to send a fixed amount of messages. It is not needed because we are now prompting for the amount of messages to be sent.
            //const int numberOfMessages = 10;
            topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

            //Console.WriteLine("=========================RUEXPSB=========================");
            //Console.WriteLine("Press ENTER key to exit after sending all the messages");
            //Console.WriteLine("==========================SEND==========================");
            //Console.WriteLine("======================================================");

            //Send messages
            await SendMessagesToTopicAsync(numberOfMessages);

            //Console.ReadKey();

            await topicClient.CloseAsync();
        }

        static async Task SendMessagesToTopicAsync(int numberOfMessagesToSend)
        {
            try
            {
                for (var i = 0; i < numberOfMessagesToSend; i++)
                {
                    //Create a new message to send to the topic
                    string messageBody = $"TopicMessage {i}";
                    var message = new Message(Encoding.UTF8.GetBytes(messageBody));

                    // Getting current time to add it to print it on screen
                    var time = DateTime.UtcNow;

                    //Write the body of the message to the console
                    Console.WriteLine($"Sending message: {messageBody} at {time}");

                    // Send the message to the topic.
                    await topicClient.SendAsync(message);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        //SEND MESSAGES TO SERVICE BUS TOPIC

        //RECEIVE MESSAGES FROM SERVICE BUS SUBSCRIPTION
                static async Task ReceiveFromSubscriptionMainAsync()
        {
            Console.WriteLine("======================================================");
            Console.Write("Enter the Subcription you want to receive mesages from (s1, s2 or s3):");
            string SubscriptionName = Console.ReadLine();

            subscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, SubscriptionName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages");
            
            // Register subscription message handler and receive messages in a loop
            SusbcriptionRegisterOnMessageHandlerAndReceiveMessages();

            Console.ReadKey();

            await subscriptionClient.CloseAsync();
        }

        static void SusbcriptionRegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure tha message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new MessageHandlerOptions(SusbcriptionExceptionReceivedHandler)
            {
                // Maximun number of concurrent calls to the callback ProcessMessagesAsync(), set to 1for simplicity
                // Set it according to how many messages teh application wants to process in parallel
                MaxConcurrentCalls = 1,

                // Indicates whether the message pump shoould automatically complete the messages after returning from user callback.
                //  False below indicate the complete operation is hanled by the user callback as in ProcessMessageAsync().
                AutoComplete = false
            };

            //Register the function that processes messages
            subscriptionClient.RegisterMessageHandler(SusbcriptionProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task SusbcriptionProcessMessagesAsync(Message message, CancellationToken token)
        {
            // Process the message
            Console.WriteLine($"Received message: MessageID:{message.MessageId} 2SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)} EnqueuedTimeUtc:{message.SystemProperties.EnqueuedTimeUtc} ");

            //Complete the message so it is not received again
            // This can be done only if the subscriptionClient is created in ReceiveMode.PeekLock mode (which is the default).
            await subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);

            //Note: Use the cancellationToken passe as necessary to determine if the subscriptionClient has already been Closed.
            // If subscriptionClient has already been closed, you can choose to not call CompleteAsync() or AbandonSync() etc.
            // to avoid unnecessary exceptions.
        }

        // Use this handler to examine the exceptions received on the message pump.
        static Task SusbcriptionExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
        //RECEIVE MESSAGES FROM SERVICE BUS SUBSCRIPTION

        //CHECK QUEUE STATUS
        
            // TODO: Write a better logic to do not need to statically define queues, topics and Subscriptions. Instead, read from Namespace
        static async Task CheckStatusMainAsync()
        {
            var managementClient = new ManagementClient(ServiceBusConnectionString);
            var queue = await managementClient.GetQueueRuntimeInfoAsync(QueueName);
            var sub1 = await managementClient.GetSubscriptionRuntimeInfoAsync(TopicName, "s1");
            var sub2 = await managementClient.GetSubscriptionRuntimeInfoAsync(TopicName, "s2");
            var sub3 = await managementClient.GetSubscriptionRuntimeInfoAsync(TopicName, "s3");
            var queueMessageCount = queue.MessageCount;
            var sub1MessageCount = sub1.MessageCount;
            var sub2MessageCount = sub3.MessageCount;
            var sub3MessageCount = sub3.MessageCount;
                       
            Console.WriteLine("Messages in queue: "+ queueMessageCount);
            Console.WriteLine("Messages in Sub1: " + sub1MessageCount); 
            Console.WriteLine("Messages in Sub2: " + sub2MessageCount);
            Console.WriteLine("Messages in Sub3: " + sub3MessageCount);


            // Wait for an ENTER to close
            //Console.ReadKey();
            //await managementClient.CloseAsync();

        }

        //CHECK QUEUE STATUS

    }
}
