using System.Net;
using FivetranClient.Infrastructure;

namespace FivetranClient;

public class HttpRequestHandler
{
    private const int cacheDurationInMinutes = 60;
    private const int retryDelayInSeconds = 60;
    private readonly HttpClient _client;
    private readonly SemaphoreSlim? _semaphore;
    private readonly object _lock = new();
    private DateTime _retryAfterTime = DateTime.UtcNow;
    private static TtlDictionary<string, HttpResponseMessage> _responseCache = new();

    /// <summary>
    /// Handles HttpTooManyRequests responses by limiting the number of concurrent requests and managing retry logic.
    /// Also caches responses to avoid unnecessary network calls.
    /// </summary>
    /// <remarks>
    /// Set <paramref name="maxConcurrentRequests"/> to 0 to disable concurrency limit.
    /// </remarks>
    public HttpRequestHandler(HttpClient client, ushort maxConcurrentRequests = 0)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        if (maxConcurrentRequests > 0)
        {
            _semaphore = new SemaphoreSlim(maxConcurrentRequests, maxConcurrentRequests);
        }
    }

    public async Task<HttpResponseMessage> GetAsync(string url, CancellationToken cancellationToken)
    {
        return await _responseCache.GetOrAddAsync(
            url,
            () => _GetAsync(url, cancellationToken),
            TimeSpan.FromMinutes(cacheDurationInMinutes));
    }

    private async Task<HttpResponseMessage> _GetAsync(string url, CancellationToken cancellationToken)
    {
        TimeSpan timeToWait;
        lock (_lock)
        {
            timeToWait = _retryAfterTime - DateTime.UtcNow;
        }
        if (timeToWait > TimeSpan.Zero)
        {
            await Task.Delay(timeToWait, cancellationToken);
        }

        if (_semaphore is not null)
        {
            await _semaphore.WaitAsync(cancellationToken);
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            // support for both absolute and relative URIs
            var uri = Uri.TryCreate(url, UriKind.Absolute, out var abs) ? abs : new Uri(url, UriKind.Relative);

            while (true)
            {
                var response = await _client.GetAsync(uri, cancellationToken);

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    var retryAfter = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(retryDelayInSeconds);

                    lock (_lock)
                    {
                        _retryAfterTime = DateTime.UtcNow.Add(retryAfter);
                    }

                    response.Dispose();

                    var waitTime = _retryAfterTime - DateTime.UtcNow;
                    if (waitTime > TimeSpan.Zero)
                        await Task.Delay(waitTime, cancellationToken);

                    continue;
                }

                response.EnsureSuccessStatusCode();
                return response;
            }
        }
        finally
        {
            _semaphore?.Release();
        }

    }
}