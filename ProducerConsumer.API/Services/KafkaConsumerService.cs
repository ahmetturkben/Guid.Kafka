using Confluent.Kafka;
using LZ4;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using ProducerConsumer.API.Models;
using RestService;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ProducerConsumer.API.Services
{
    public class KafkaConsumerService : ProducerConsumer.API.Services.BackgroundService, IConsumerService
    {
        private readonly ILogger<KafkaConsumerService> _logger;
        private readonly KafkaConfiguration _kafkaConfiguration;
        private IConsumer<string, string> _consumer;
        private IServiceScopeFactory _serviceScopeFactory;

        public KafkaConsumerService(ILogger<KafkaConsumerService> logger, IOptions<KafkaConfiguration> kafkaConfigurationOptions
            //, IScheduleConfig<KafkaConsumerService> config
            ,IServiceScopeFactory serviceScopeFactory
            ) : base(null, null)
        {
            _logger = logger ?? throw new ArgumentException(nameof(logger));
            _kafkaConfiguration = kafkaConfigurationOptions?.Value ?? throw new ArgumentException(nameof(kafkaConfigurationOptions));
            _serviceScopeFactory = serviceScopeFactory;

            Init();
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Kafka Consumer Service has started.");

                    _consumer.Subscribe(new List<string>() { _kafkaConfiguration.Topic });

                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = _consumer.Consume(cancellationToken);

                            if (consumeResult?.Message == null) continue;

                            if (consumeResult.Topic.Equals(_kafkaConfiguration.Topic))
                            {

                                Task.Run(() =>
                                {
                                    using (var scope = _serviceScopeFactory.CreateScope())
                                    {
                                        var reportId = Encoding.UTF8.GetString(LZ4Codec.Unwrap(Convert.FromBase64String(consumeResult.Message.Value)));
                                        if (!string.IsNullOrEmpty(reportId))
                                        {
                                            var service = scope.ServiceProvider.GetService<IReportService>();
                                            var result = service.CompletedPutData("/ReportCompleted", reportId);
                                        }

                                        _logger.LogInformation($"[{consumeResult.Message.Key}] {consumeResult.Topic} - {reportId}");
                                    }
                                }, cancellationToken).ConfigureAwait(false);

                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, ex.Message);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }

            return base.StartAsync(cancellationToken);
        }

        public override Task DoWork(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Kafka Consumer Service has started.");

                    _consumer.Subscribe(new List<string>() { _kafkaConfiguration.Topic });

                    //Consume(cancellationToken).ConfigureAwait(false);
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        try
                        {
                            var consumeResult = _consumer.Consume(cancellationToken);

                            if (consumeResult?.Message == null) continue;

                            if (consumeResult.Topic.Equals(_kafkaConfiguration.Topic))
                            {
                                using (var scope = _serviceScopeFactory.CreateScope())
                                {
                                    Task.Run(() =>
                                {
                                    var json = Encoding.UTF8.GetString(LZ4Codec.Unwrap(Convert.FromBase64String(consumeResult.Message.Value)));

                                    var service = scope.ServiceProvider.GetService<IReportService>();
                                    var result = service.CompletedPutData("/ReportCompleted", json);


                                    _logger.LogInformation($"[{consumeResult.Message.Key}] {consumeResult.Topic} - {json}");
                                }, cancellationToken).ConfigureAwait(false);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, ex.Message);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }

            return Task.CompletedTask;
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Kafka Consumer Service is stopping.");

            _consumer.Close();

            return base.StopAsync(cancellationToken);
        }

        public override void Dispose()
        {
            _consumer.Dispose();
        }

        private void Init()
        {
            //var pemFileWithKey = "./keystore/secure.pem";

            var config = new ConsumerConfig()
            {
                BootstrapServers = _kafkaConfiguration.Brokers,

                //SslCaLocation = pemFileWithKey,
                //SslCertificateLocation = pemFileWithKey,
                //SslKeyLocation = pemFileWithKey,

                //Debug = "broker,topic,msg",

                GroupId = _kafkaConfiguration.ConsumerGroup,
                SecurityProtocol = SecurityProtocol.Plaintext,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            _consumer = new ConsumerBuilder<string, string>(config).SetStatisticsHandler((_, kafkaStatistics) => LogKafkaStats(kafkaStatistics)).
                SetErrorHandler((_, e) => LogKafkaError(e)).Build();
        }

        private void LogKafkaStats(string kafkaStatistics)
        {
            var stats = JsonConvert.DeserializeObject<KafkaStatistics>(kafkaStatistics);

            if (stats?.topics != null && stats.topics.Count > 0)
            {
                foreach (var topic in stats.topics)
                {
                    foreach (var partition in topic.Value.Partitions)
                    {
                        Task.Run(() =>
                        {
                            var logMessage = $"FxRates:KafkaStats Topic: {topic.Key} Partition: {partition.Key} PartitionConsumerLag: {partition.Value.ConsumerLag}";
                            _logger.LogInformation(logMessage);
                        });
                    }
                }
            }
        }

        private void LogKafkaError(Error ex)
        {
            Task.Run(() =>
            {
                var error = $"Kafka Exception: ErrorCode:[{ex.Code}] Reason:[{ex.Reason}] Message:[{ex.ToString()}]";
                _logger.LogError(error);
            });
        }
    }
}
