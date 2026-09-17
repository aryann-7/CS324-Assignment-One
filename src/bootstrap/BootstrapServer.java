package bootstrap;

import common.interfaces.BootstrapService;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

//Standalone entry point for the Bootstrap Node.
//Starts the RMI registry (if not already running) and binds BootstrapService.

public class BootstrapServer {

    public static final int DEFAULT_PORT = 1099;
    public static final String BIND_NAME = "BootstrapService";

    public static void main(String[] args) {
        // Method stub: parse CLI arguments, instantiate BootstrapServiceImpl, and bind
        // to RMI registry
    }
}
