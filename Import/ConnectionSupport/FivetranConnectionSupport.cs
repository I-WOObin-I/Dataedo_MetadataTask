using FivetranClient;
using FivetranClient.Models;
using Import.Helpers.Fivetran;
using System.Collections.Concurrent;
using System.Runtime.ConstrainedExecution;
using System.Text;

namespace Import.ConnectionSupport;

// equivalent of database is group in Fivetran terminology
public class FivetranConnectionSupport : IConnectionSupport
{
    private const int timeoutInSeconds = 40;
    public const string ConnectorTypeCode = "FIVETRAN";
    private record FivetranConnectionDetailsForSelection(string ApiKey, string ApiSecret);

    public object? GetConnectionDetailsForSelection()
    {
        var apiKey = PromptValidString("Provide your Fivetran API Key: ");
        var apiSecret = PromptValidString("Provide your Fivetran API Secret: ");

        return new FivetranConnectionDetailsForSelection(apiKey, apiSecret);
    }

    public object GetConnection(object? connectionDetails, string? selectedToImport)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
        {
            throw new ArgumentException("Invalid connection details provided.");
        }

        return new RestApiManagerWrapper(
            new RestApiManager(
                details.ApiKey,
                details.ApiSecret,
                TimeSpan.FromSeconds(timeoutInSeconds)),
            selectedToImport ?? throw new ArgumentNullException(nameof(selectedToImport)));
    }

    public void CloseConnection(object? connection)
    {
        switch (connection)
        {
            case RestApiManager restApiManager:
                restApiManager.Dispose();
                break;
            case RestApiManagerWrapper restApiManagerWrapper:
                restApiManagerWrapper.Dispose();
                break;
            default:
                throw new ArgumentException("Invalid connection type provided.");
        }
    }

    public string SelectToImport(object? connectionDetails)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
        {
            throw new ArgumentException("Invalid connection details provided.");
        }
        using var restApiManager = new RestApiManager(details.ApiKey, details.ApiSecret, TimeSpan.FromSeconds(timeoutInSeconds));
        var groups = restApiManager
            .GetGroupsAsync(CancellationToken.None)
            .ToBlockingEnumerable()
            .ToList();

        if (groups.Count == 0)
        {
            throw new Exception("No groups found in Fivetran account.");
        }

        // bufforing with StringBuilder for performance
        var consoleOutputBuffer = new StringBuilder("Available groups in Fivetran account:\n");
        var elemId = 1;
        foreach (var group in groups)
        {
            consoleOutputBuffer.AppendLine($"{elemId++}. {group.Name} (ID: {group.Id})");
        }
        Console.WriteLine(consoleOutputBuffer);

        var input = PromptValidString("Please select a group to import from (by number): ");
        if (string.IsNullOrWhiteSpace(input)
            || !int.TryParse(input, out var selectedIndex)
            || selectedIndex < 1
            || selectedIndex > groups.Count())
        {
            throw new ArgumentException("Invalid group selection.");
        }

        var selectedGroup = groups.ElementAt(selectedIndex - 1);
        return selectedGroup.Id;
    }

    public void RunImport(object? connection)
    {
        if (connection is not RestApiManagerWrapper restApiManagerWrapper)
        {
            throw new ArgumentException("Invalid connection type provided.");
        }

        var restApiManager = restApiManagerWrapper.RestApiManager;
        var groupId = restApiManagerWrapper.GroupId;

        var connectors = restApiManager
            .GetConnectorsAsync(groupId, CancellationToken.None)
            .ToBlockingEnumerable()
            .ToList();

        if (!connectors.Any())
        {
            throw new Exception("No connectors found in the selected group.");
        }

        var mappingsBuffer = new ConcurrentBag<string>();
        Parallel.ForEachAsync(connectors, async(connector, ct) =>
        {
            var connectorSchemas = await restApiManager
                .GetConnectorSchemasAsync(connector.Id, ct)
                .ConfigureAwait(false);

            foreach (var schema in connectorSchemas?.Schemas ?? [])
            {
                foreach (var table in schema.Value?.Tables ?? [])
                {
                    mappingsBuffer.Add($"  {connector.Id}: {schema.Key}.{table.Key} -> {schema.Value?.NameInDestination}.{table.Value.NameInDestination}\n");
                }
            }
        });

        var allMappingsBuffer = "Lineage mappings:\n" + mappingsBuffer.ToList();
        Console.WriteLine(allMappingsBuffer);
    }

    private static string PromptValidString(string prompt)
    {
        Console.Write(prompt);
        var input = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(input))
        {
            Console.WriteLine("input null or white space");
            throw new ArgumentNullException("input null or white space");
        }
        return input;
    }
}