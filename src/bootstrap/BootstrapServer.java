package bootstrap;

import common.interfaces.BootstrapService;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;

/**
 * Starts the Bootstrap Node.
 */
public class BootstrapServer {

    public static final int DEFAULT_PORT = 1099;

    public static final String BIND_NAME =
            "BootstrapService";

    public static void main(String[] args) {
        if (args.length > 1) {
            System.err.println("Usage: BootstrapServer [port]");
            return;
        }

        int port = DEFAULT_PORT;
        if (args.length == 1) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Bootstrap port must be an integer between 1 and 65535");
                return;
            }

            if (port <= 0 || port > 65535) {
                System.err.println("Bootstrap port must be between 1 and 65535");
                return;
            }
        }

        try {
            Registry registry;
            try {
                registry = LocateRegistry.createRegistry(port);
            } catch (ExportException e) {
                registry = LocateRegistry.getRegistry(port);
                registry.list();
            }

            BootstrapService bootstrapService = new BootstrapServiceImpl();
            registry.rebind(BIND_NAME, bootstrapService);
            System.out.println(BIND_NAME + " running on port " + port);
        } catch (RemoteException e) {
            System.err.println("Failed to start BootstrapService: " + e.getMessage());
            System.exit(1);

        try {

            int port = DEFAULT_PORT;

            if (args.length > 0) {

                port =
                        Integer.parseInt(args[0]);
            }

            Registry registry;

            try {

                registry =
                        LocateRegistry.createRegistry(
                                port);

            } catch (Exception e) {

                registry =
                        LocateRegistry.getRegistry(
                                port);
            }

            BootstrapService service =
                    new BootstrapServiceImpl();

            registry.rebind(
                    BIND_NAME,
                    service);

            System.out.println(
                    "================================");

            System.out.println(
                    "Bootstrap Node started.");

            System.out.println(
                    "RMI port: " + port);

            System.out.println(
                    "Service: " + BIND_NAME);

            System.out.println(
                    "================================");

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}