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
        static void Main(string[] args)
        {
            int interval = 1000;
            bool parsed = false;
            if(args.Any())
                parsed = Int32.TryParse(args[0], out interval);

            var conf = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
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

                producer.Produce("demo", new Message<Null, string> { Value = $"Message{counter}" }, (message) =>
                {
                    Console.WriteLine($"Message with content {message.Value} sended successfully");
                });
    
                Thread.Sleep(TimeSpan.FromMilliseconds(parsed ? interval : 1000));
            }
        }
    }
}
