/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 * Patent Pending
 *
 * Kuber Distributed Cache - .NET Examples
 * Main Entry Point
 */

using Kuber.Client.Examples;

Console.WriteLine(@"
╔══════════════════════════════════════════════════════════════════════╗
║               KUBER DISTRIBUTED CACHE - C# EXAMPLES                  ║
║                          Version 1.8.3                               ║
╚══════════════════════════════════════════════════════════════════════╝

Select an example to run:

  1. Redis Protocol   - High-performance access via StackExchange.Redis
  2. REST API         - HTTP-based access with API key authentication
  3. Messaging        - Request/Response messaging via message brokers

Enter your choice (1-3), or 'q' to quit: ");

while (true)
{
    var input = Console.ReadLine()?.Trim().ToLower();
    
    switch (input)
    {
        case "1":
        case "redis":
            Console.Clear();
            await RedisProtocolExample.RunAsync();
            return;
            
        case "2":
        case "rest":
            Console.Clear();
            await RestApiExample.RunAsync();
            return;
            
        case "3":
        case "messaging":
            Console.Clear();
            MessagingExample.Run();
            return;
            
        case "q":
        case "quit":
        case "exit":
            Console.WriteLine("\nGoodbye!");
            return;
            
        default:
            Console.Write("\nInvalid choice. Enter 1, 2, 3, or 'q' to quit: ");
            break;
    }
}
