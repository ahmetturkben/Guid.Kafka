using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Producer.API.Services;
using ProducerConsumer.API.Models;
using ProducerConsumer.API.Services;
using RestService;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Producer.API
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddHttpClient();
            //services.AddSingleton<IProducerService, KafkaProducerService>();
            services.Configure<KafkaConfiguration>(Configuration.GetSection(nameof(KafkaConfiguration)));
            services.AddScoped<IReportService, ReportService>();

            services.AddCronJob<KafkaProducerService>(c =>
            {
                c.TimeZoneInfo = TimeZoneInfo.Local;
                c.CronExpression = @"*/1 * * * *";
            });

            //KafkaConsumerService  service i�in schedule �al??mas?na gerek olmad???, zaten kafka s�rekli dinleme modunda oldu?u i�in, addhostedService olarak inject etmek yeterlidir.
            services.AddHostedService<KafkaConsumerService>();
            //services.AddCronJob<KafkaConsumerService>(c =>
            //{
            //    c.TimeZoneInfo = TimeZoneInfo.Local;
            //    c.CronExpression = @"*/2 * * * *";
            //});

            services.AddControllers();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "Producer.API", Version = "v1" });
            });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseSwagger();
                app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "Producer.API v1"));
            }

            app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
