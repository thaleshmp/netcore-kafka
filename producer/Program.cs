using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Public;
using Kafka.Public.Loggers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace producer
{
    class Program
    {
        static (string kafkaServer, int interval, string message) GetConfiguration()
        {
            Console.Write("Kafka Server: ");
            var server = Console.ReadLine();

            Console.Write("Interval between messages: ");
            var interval = Console.ReadLine();

            Console.Write("Message to broadcast: ");
            var message = Console.ReadLine();

            return (server, Int32.Parse(interval), message);
        }

        static void Main(string[] args)
        {
            var kafkaServer = "localhost:9092";
            var messagingInterval = 500;
            var message = "Message{counter}";

            if(!(args.Any() && args[0] == "default"))
            {
                var config = GetConfiguration();
                kafkaServer = config.kafkaServer;
                messagingInterval = config.interval;
                message = config.message;
            }

            var messageHasCounter = message.Contains("{counter}");

            var conf = new ProducerConfig
            {
                BootstrapServers = kafkaServer
            };

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                Console.WriteLine("we just captured the process exit."); // <-- this is never reached.
                e.Cancel = true;
                cts.Cancel();
            };

            using var producer = new ProducerBuilder<Null, string>(conf).Build();

            int counter = 0;
            while (!cts.IsCancellationRequested)
            {
                counter++;

                var messageToSend = messageHasCounter ? 
                    message.Replace("{counter}", counter.ToString()) :
                    message;

                producer.Produce("demo", new Message<Null, string> { Value = messageToSend }, (message) =>
                {
                    Console.WriteLine($"({counter}) Message with content {message.Value} sended successfully");
                });
    
                Thread.Sleep(TimeSpan.FromMilliseconds(messagingInterval));
            }
        }
    }
}
