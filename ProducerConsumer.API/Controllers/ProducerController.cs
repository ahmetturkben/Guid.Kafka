using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using ProducerConsumer.API.Models;
using ProducerConsumer.API.Services;

namespace Producer.API.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ProducerController : Controller
    {
        public IProducerService _producerService { get; set; }
        public ProducerController(IProducerService producerService)
        {
            _producerService = producerService;
        }

        [HttpPost]
        public IActionResult Post([FromBody] ReportDto model)
        {
            var report = model;
            
            return Ok();
        }
    }
}
