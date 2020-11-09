using System;
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
            CreateHostBuilder(args).Build().Run();
        }

        private static IHostBuilder CreateHostBuilder(string[] args) => 
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, collection) => {
                    // collection.AddHostedService<KafkaConsumerHostedService>();
                    collection.AddHostedService<KafkaProducerHostedService>();
                });
    }

    public class KafkaProducerHostedService : IHostedService
    {
        private readonly ILogger<KafkaProducerHostedService> _logger;
        private IProducer<Null, string> _producer;

        public KafkaProducerHostedService(ILogger<KafkaProducerHostedService> logger)
        {
            this._logger = logger;
            var config = new ProducerConfig(){
                BootstrapServers = "localhost:9092"
            };
            this._producer = new ProducerBuilder<Null, string>(config).Build();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            for(var i = 0; i < 10; i++)
            {
                var messageToSend = $"Hello World, we're on message {i}";
                this._logger.LogInformation(messageToSend);
                await this._producer.ProduceAsync("demo", new Message<Null, string>(){
                    Value = messageToSend
                }, cancellationToken);
            }

            _producer.Flush();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            this._producer?.Dispose();
            Console.WriteLine($"Producer on class {nameof(KafkaProducerHostedService)} disposed.");
            return Task.CompletedTask;
        }
    }

    public class KafkaConsumerHostedService : IHostedService
    {
        private readonly ILogger<KafkaConsumerHostedService> _logger;
        private readonly ClusterClient _cluster;

        public KafkaConsumerHostedService(ILogger<KafkaConsumerHostedService> logger)
        {
            this._logger = logger;
            this._cluster = new ClusterClient(new Configuration(){
                Seeds = "localhost:9092"
            }, new ConsoleLogger());
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _cluster.ConsumeFromLatest(topic: "demo");
            _cluster.MessageReceived += record => 
            {
                _logger.LogInformation($"Received: {Encoding.UTF8.GetString(record.Value as byte[])}");
            };
            _cluster.MessageDiscarded += record => 
            {
                _logger.LogInformation($"Discarded: {Encoding.UTF8.GetString(record.Value as byte[])}");
            };
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            this._cluster?.Dispose();
            Console.WriteLine($"Producer on class {nameof(KafkaConsumerHostedService)} disposed.");
            return Task.CompletedTask;
        }
    }
}
