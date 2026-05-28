import mongoose from "mongoose";

const proxySchema = new mongoose.Schema(
  {
    ip: { type: String, required: true },
    port: { type: String, required: true },
    username: { type: String, required: true },
    password: { type: String, required: true },
    clientId: { type: String, default: "default", index: true },
    enabled: { type: Boolean, default: true, index: true },
    notes: { type: String, default: "" },
    failureCount: { type: Number, default: 0 },
    lastUsedAt: { type: Date, default: null },
    lastFailureAt: { type: Date, default: null },
  },
  { timestamps: true }
);

proxySchema.index({ ip: 1, port: 1 }, { unique: true });

export const Proxy = mongoose.model("Proxy", proxySchema);
export default Proxy;
