import {ScrapeEvent} from "./scraper.js";
import moment from "moment";
import { writeFile } from "fs/promises";

 const result = await ScrapeEvent({
      eventId: "0A00614FC8B22987",
    });




await writeFile("result.json", JSON.stringify(result, null, 2), "utf-8");
