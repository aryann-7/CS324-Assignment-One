package bootstrap;

import common.interfaces.BootstrapService;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Starts the Bootstrap Node.
 */
public class BootstrapServer {

    public static final int DEFAULT_PORT = 1099;

    public static final String BIND_NAME =
            "BootstrapService";

    public static void main(String[] args) {

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