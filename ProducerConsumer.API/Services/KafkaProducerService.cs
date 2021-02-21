using Confluent.Kafka;
using LZ4;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using ProducerConsumer.API.Models;
using ProducerConsumer.API.Services;
using RestService;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Producer.API.Services
{
    public class KafkaProducerService : ProducerConsumer.API.Services.BackgroundService, IProducerService
    {
        private IProducer<string, string> _producer;
        private readonly ILogger<KafkaProducerService> _logger;
        private readonly KafkaConfiguration _kafkaConfiguration;
        private IServiceScopeFactory _serviceScopeFactory;

        public KafkaProducerService(ILogger<KafkaProducerService> logger, IOptions<KafkaConfiguration> kafkaConfigurationOptions
            ,IScheduleConfig<KafkaProducerService> config,
            IServiceScopeFactory serviceScopeFactory
            ) : base(config.CronExpression, config.TimeZoneInfo)
        {
            _logger = logger ?? throw new ArgumentException(nameof(logger));
            _kafkaConfiguration = kafkaConfigurationOptions?.Value ?? throw new ArgumentException(nameof(kafkaConfigurationOptions));
            _serviceScopeFactory = serviceScopeFactory;

            Init();
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Kafka Producer Service has started.");
                    //Produce(cancellationToken, "").ConfigureAwait(false);
                    try
                    {
                        using (_logger.BeginScope("Kafka App Produce Sample Data"))
                        {
                            if (!cancellationToken.IsCancellationRequested)
                            {
                                using (var scope = _serviceScopeFactory.CreateScope())
                                {
                                    var service = scope.ServiceProvider.GetService<IReportService>();
                                    string data = service.GetData("/ReportProccessing");
                                    var reports = JsonConvert.DeserializeObject<List<string>>(data);
                                    for (int i = 0; i < reports.Count; i++)
                                    {
                                        var reportId = reports[i];

                                        var msg = new Message<string, string>
                                        {
                                            Key = _kafkaConfiguration.Key,
                                            Value = Convert.ToBase64String(LZ4Codec.Wrap(Encoding.UTF8.GetBytes(reportId)))
                                        };
                                        _producer.ProduceAsync(_kafkaConfiguration.Topic, msg).ConfigureAwait(false);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception exception)
                    {
                        _logger.LogError(exception, exception.Message);
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
            if (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Kafka Producer Service has started.");
                    //Produce(cancellationToken, "").ConfigureAwait(false);
                    try
                    {
                        using (_logger.BeginScope("Kafka App Produce Sample Data"))
                        {
                            if (!cancellationToken.IsCancellationRequested)
                            {
                                using (var scope = _serviceScopeFactory.CreateScope())
                                {
                                    var service = scope.ServiceProvider.GetService<IReportService>();
                                    string data = service.GetData("/ReportProccessing");
                                    var reports = JsonConvert.DeserializeObject<List<string>>(data);
                                    for (int i = 0; i < reports.Count; i++)
                                    {
                                        var reportId = reports[i];

                                        var msg = new Message<string, string>
                                        {
                                            Key = _kafkaConfiguration.Key,
                                            Value = Convert.ToBase64String(LZ4Codec.Wrap(Encoding.UTF8.GetBytes(reportId)))
                                        };
                                        _producer.ProduceAsync(_kafkaConfiguration.Topic, msg).ConfigureAwait(false);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception exception)
                    {
                        _logger.LogError(exception, exception.Message);
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
            _logger.LogInformation("Kafka Producer Service is stopping.");

            _producer.Flush(cancellationToken);
            return base.StopAsync(cancellationToken);
        }

        public override void Dispose()
        {
            _producer.Dispose();
        }

        private void Init()
        {
            //var pemFileWithKey = "./keystore/secure.pem";

            var config = new ProducerConfig()
            {
                BootstrapServers = _kafkaConfiguration.Brokers,
                ClientId = "Kafka.Dotnet.Sample",

                //SslCaLocation = pemFileWithKey,
                //SslCertificateLocation = pemFileWithKey,
                //SslKeyLocation = pemFileWithKey,

                //Debug = "broker,topic,msg",

                SecurityProtocol = SecurityProtocol.Plaintext,
                EnableDeliveryReports = false,
                QueueBufferingMaxMessages = 10000000,
                QueueBufferingMaxKbytes = 100000000,
                BatchNumMessages = 500,
                Acks = Acks.None,
                DeliveryReportFields = "none"
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }
    }
}
