import { HttpsProxyAgent } from "https-proxy-agent";
import axios from "axios";

const getProxy = () => {
  const proxyConfig = {
    host: "pr.oxylabs.io",
    port: "7777",
    username: "customer-event60822_DuSNg-sessid-0835629560-sesstime-1",
    password: "Aa1+Ba2~Cb3=Dc4",
  };

  try {
    const proxyUrl = `http://${proxyConfig.username}:${proxyConfig.password}@${proxyConfig.host}:${proxyConfig.port}`;
    const proxyAgent = new HttpsProxyAgent(proxyUrl);

    console.log(
      "Proxy URL created:",
      proxyUrl.replace(proxyConfig.password, "****")
    );

    return { proxyAgent, proxy: proxyConfig };
  } catch (error) {
    console.error("Failed to create proxy:", error.message);
    throw error;
  }
};

const testProxy = async () => {
  try {
    const { proxyAgent } = getProxy();

    console.log("Testing proxy connection...");

    const response = await axios.get("https://api.ipify.org?format=json", {
      httpsAgent: proxyAgent,
      timeout: 10000,
    });

    console.log("Proxy IP:", response.data.ip);
    console.log("Proxy Status:", response.status);
    console.log("Test successful!");

    return true;
  } catch (error) {
    console.error("Proxy test failed:", error.message);
    return false;
  }
};

// Run the test
testProxy()
  .then((result) => {
    console.log("Final result:", result);
  })
  .catch((error) => {
    console.error("Test failed:", error.message);
  });
