# Kuber Messaging Test Clients (C#)

These are standalone test clients for testing Kuber's Request/Response messaging feature with different message brokers.

## Important Note

These files are **not compiled** as part of the main build because they require external NuGet packages that may not be available in all environments.

## NuGet Packages Required

To compile and run these examples, add the following packages to your project:

### Kafka
```bash
dotnet add package Confluent.Kafka
```

### ActiveMQ
```bash
dotnet add package Apache.NMS.ActiveMQ
```

### RabbitMQ
```bash
dotnet add package RabbitMQ.Client
```

### IBM MQ
```bash
dotnet add package IBMMQDotnetClient
```

## How to Use

1. Create a new .NET console project:
   ```bash
   dotnet new console -n KuberMessagingTest
   cd KuberMessagingTest
   ```

2. Add required NuGet packages

3. Copy the desired test file to your project

4. Update the configuration constants (broker URLs, API keys, etc.)

5. Run the test:
   ```bash
   dotnet run
   ```

## Test Files

| File | Broker | Protocol |
|------|--------|----------|
| `KafkaRequestResponseTest.cs` | Apache Kafka | Native Kafka |
| `ActiveMqRequestResponseTest.cs` | Apache ActiveMQ | NMS/OpenWire |
| `RabbitMqRequestResponseTest.cs` | RabbitMQ | AMQP |
| `IbmMqRequestResponseTest.cs` | IBM MQ | IBM MQ .NET |

## Configuration

Each test file has configuration constants at the top that you need to update:

```csharp
// Broker connection
private const string BootstrapServers = "localhost:9092";

// Kuber API key
private const string ApiKey = "kub_your_api_key_here";

// Queue/Topic names
private const string RequestTopic = "ccs_cache_request";
private const string ResponseTopic = "ccs_cache_response";
```

## Example Project File

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
  </PropertyGroup>
  
  <ItemGroup>
    <!-- Choose the packages you need -->
    <PackageReference Include="Confluent.Kafka" Version="2.2.0" />
    <PackageReference Include="Apache.NMS.ActiveMQ" Version="2.2.0" />
    <PackageReference Include="RabbitMQ.Client" Version="6.8.1" />
    <PackageReference Include="IBMMQDotnetClient" Version="9.3.4" />
  </ItemGroup>
</Project>
```

## Running Tests

```bash
# Build and run
dotnet build
dotnet run

# Or run directly
dotnet run --project YourTestProject.csproj
```
