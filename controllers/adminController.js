import { spawn } from 'child_process';
import path from 'path';

export const restartServer = (req, res) => {
  console.log('Received request to restart server.');

  // Respond to the client immediately as this process will exit.
  res.status(200).json({ message: 'Server restart initiated. Please wait a moment and refresh.' });

  // Perform graceful shutdown if your server instance is accessible here
  // For example, if 'server' is your http.createServer() instance:
  // if (global.serverInstance) {
  //   global.serverInstance.close(() => {
  //     console.log('HTTP server closed.');
  //     proceedWithRestart();
  //   });
  // } else {
  //   proceedWithRestart();
  // }
  // For simplicity, we'll proceed directly. Add graceful shutdown as needed.
  proceedWithRestart();
};

function proceedWithRestart() {
  console.log('Proceeding with server restart...');

  const mainScript = path.join(process.cwd(), 'app.js'); // Assuming app.js is your main file

  // Spawn a new process
  const child = spawn(process.execPath, [mainScript], {
    detached: true,
    stdio: 'ignore', // or 'inherit' to see logs from the new process in the current console
  });

  child.unref(); // Allow parent process to exit independently

  console.log(`New process spawned with PID: ${child.pid}. Exiting current process.`);
  
  // Exit the current process
  // A small delay might help ensure the response is sent and logs are flushed
  setTimeout(() => {
    process.exit(0);
  }, 1000); 
}
