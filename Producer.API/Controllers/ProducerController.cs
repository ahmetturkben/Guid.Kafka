using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using ProducerConsumer.API.Models;

namespace Producer.API.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ProducerController : Controller
    {
        [HttpPost]
        public IActionResult Post([FromBody] ReportDto model)
        {
            var report = model;

            
            return Ok();
        }
    }
}
