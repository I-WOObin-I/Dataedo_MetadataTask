namespace FivetranClient.Models;

public class Data<T>
{
    public List<T> Items { get; set; } = new();
    public string? NextCursor { get; set; }
}