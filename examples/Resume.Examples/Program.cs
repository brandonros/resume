using Resume;
using Resume.Examples;

using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; stop.Cancel(); };
try
{
    var command = args.FirstOrDefault() ?? "help";
    switch (command)
    {
        case "install":
            await using (var connection = await Host.ConnectAsync(ct: stop.Token))
                await Schema.InstallAsync(connection, stop.Token);
            break;
        case "basic": await Demos.Basic(stop.Token); break;
        case "flow": await Demos.Flow(stop.Token); break;
        case "recovery": await Demos.Recovery(stop.Token); break;
        case "orders": await Orders.RunAsync(args[1..], stop.Token); break;
        case "importer": await Importer.RunAsync(args[1..], stop.Token); break;
        case "compensation": await Compensation.RunAsync(args[1..], stop.Token); break;
        default: Console.WriteLine("Commands: install | basic | flow | recovery | orders | importer | compensation"); break;
    }
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
