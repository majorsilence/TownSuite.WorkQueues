namespace TownSuite.WorkQueues
{
    public class MessageDto
    {
        public int Id { get; set; }
        public DateTime TimeCreatedUtc { get; set; }
        public string Channel { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public DateTime? TimeProcessedUtc { get; set; }
    }
}
