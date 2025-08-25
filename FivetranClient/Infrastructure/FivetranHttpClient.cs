using System.Net.Http.Headers;
using System.Text;

namespace FivetranClient.Infrastructure;

public class FivetranHttpClient : HttpClient
{
    private static readonly string UserAgentHeader = "Fivetran_Client_id_21";
    private static readonly int timeoutInSeconds = 40;
    public FivetranHttpClient(Uri baseAddress, string apiKey, string apiSecret, TimeSpan timeout)
    {
        if (timeout.Ticks <= 0)
            throw new ArgumentOutOfRangeException(nameof(timeout), "Timeout must be a positive value");
        if (baseAddress == null)
            throw new ArgumentNullException(nameof(baseAddress));
        if (string.IsNullOrWhiteSpace(apiKey))
            throw new ArgumentException("API key cannot be null or whitespace.", nameof(apiKey));
        if (string.IsNullOrWhiteSpace(apiSecret))
            throw new ArgumentException("API secret cannot be null or whitespace.", nameof(apiSecret));

        DefaultRequestHeaders.Clear();
        BaseAddress = baseAddress;
        DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Basic", CalculateToken(apiKey, apiSecret));
        DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        // we need to set Agent Header because otherwise sometimes it may be blocked by the server
        // see: https://repost.aws/knowledge-center/waf-block-http-requests-no-user-agent
        DefaultRequestHeaders.UserAgent.ParseAdd(UserAgentHeader);
        Timeout = timeout;
    }

    public FivetranHttpClient(Uri baseAddress, string apiKey, string apiSecret)
        : this(baseAddress, apiKey, apiSecret, TimeSpan.FromSeconds(timeoutInSeconds))
    {
    }

    private static string CalculateToken(string apiKey, string apiSecret)
    {
        return Convert.ToBase64String(Encoding.ASCII.GetBytes($"{apiKey}:{apiSecret}"));
    }
}