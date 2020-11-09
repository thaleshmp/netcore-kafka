using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Confluent.Kafka;

namespace sender
{
    class Program
    {
        public static CancellationToken _cancelationToken;
        public static IConsumer<Ignore, string> consumer;

        static void Main(string[] args)
        {
            Console.CancelKeyPress += CurrentDomain_ProcessExit;

            var config = new List<KeyValuePair<string, string>>{
                new KeyValuePair<string, string>("group.id", "consumer1"),
                new KeyValuePair<string, string>("bootstrap.servers", "localhost:9092"),
                new KeyValuePair<string, string>("enable.auto.commit", "false")
            };

            Program.consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            Program.consumer.Subscribe(new string[] { "demo" });

            while (true)
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Message Received: {consumeResult.Message.Value}");
            }
        }

        internal static void CurrentDomain_ProcessExit(object sender, EventArgs e)
        {
            Program.consumer.Close();
            Console.WriteLine("we just captured the process exit."); // <-- this is never reached.
        }
    }
}
