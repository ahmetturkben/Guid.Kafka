using System;

namespace ProducerConsumer.API.Models
{
    public class ReportDto
    {
        public string Id { get; set; }
        public DateTime RequestDate { get; set; }
        public string ReportStatus { get; set; }
    }
}
