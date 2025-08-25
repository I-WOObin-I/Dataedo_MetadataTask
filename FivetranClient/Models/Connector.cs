namespace FivetranClient.Models;

public class Connector
{
    public required string Id { get; set; }
    public required string Service { get; set; }
    public required string Schema { get; set; }
    public bool? Paused { get; set; }
}