// An outside client against a real nwc4ln. Our Go test client could share our
// misreading of the protocol, and this one cannot. test/integration/nwc starts the
// service and runs this script. lnd-b is the outside party that pays and gets paid.
//
// Every check must pass. A failed check fails the Go test.
import { NWCClient } from "@getalby/sdk/nwc";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";

const { NWC_FIRST, NWC_SECOND, NWC_SECOND_LIMIT, REGTEST } = process.env;
const lnb = (...args) => execFileSync(REGTEST, ["lncli", "lnd-b", ...args], { encoding: "utf8", timeout: 60_000 });
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const makeOutsideInvoice = (sats) => JSON.parse(lnb("addinvoice", "--amt", String(sats))).payment_request;

let failed = 0;
async function check(name, run) {
  try {
    console.log("PASS", name, JSON.stringify(await run())?.slice(0, 150) ?? "");
  } catch (error) {
    failed++;
    console.log("FAIL", name, error.code ?? "", String(error.message).slice(0, 300));
  }
}
function must(condition, message) {
  if (!condition) throw new Error(message);
}
async function until(what, ready) {
  for (let i = 0; i < 100; i++) {
    const value = await ready();
    if (value) return value;
    await sleep(200);
  }
  throw new Error("timed out: " + what);
}
const preimageFits = (preimage, hash) =>
  createHash("sha256").update(Buffer.from(preimage, "hex")).digest("hex") === hash;

const first = new NWCClient({ nostrWalletConnectUrl: NWC_FIRST });
const second = new NWCClient({ nostrWalletConnectUrl: NWC_SECOND });

const notes = [];
const unsubscribe = await first.subscribeNotifications((note) => notes.push(note), ["payment_received", "payment_sent"]);
await sleep(1_000); // The relay drops a notification nobody is listening for.
const noted = (type, hash) => notes.find((n) => n.notification_type === type && n.notification.payment_hash === hash);

await check("info event", async () => {
  const info = await first.getWalletServiceInfo();
  must(info.encryptions.includes("nip44_v2"), "no nip44_v2");
  must(info.notifications.includes("payment_sent"), "no notifications");
  return info.capabilities;
});
await check("get_info", async () => {
  const info = await first.getInfo();
  must(info.network === "regtest" && info.methods.includes("make_invoice"), JSON.stringify(info));
  return info.methods;
});
await check("get_balance", async () => {
  const { balance } = await first.getBalance();
  must(balance > 0, "balance " + balance);
  return balance;
});

const invoice = await first.makeInvoice({ amount: 1_000_000, description: "acceptance" });
await check("make_invoice", async () => {
  must(invoice.type === "incoming" && invoice.state === "pending", invoice.state);
  must(invoice.amount === 1_000_000 && invoice.invoice.startsWith("lnbcrt"), invoice.invoice);
  must(!invoice.preimage, "shows the preimage of an unpaid invoice");
  return invoice.payment_hash;
});
await check("receive, and lookup_invoice settled", async () => {
  lnb("payinvoice", "--force", "--json", invoice.invoice);
  const found = await until("settled", async () => {
    const result = await first.lookupInvoice({ payment_hash: invoice.payment_hash });
    return result.settled_at ? result : null;
  });
  must(found.state === "settled" && preimageFits(found.preimage, invoice.payment_hash), found.preimage);
  return found.state;
});

const outside = makeOutsideInvoice(2_000);
let sent;
await check("pay_invoice to lnd-b", async () => {
  sent = await first.payInvoice({ invoice: outside });
  must(sent.preimage.length === 64 && typeof sent.fees_paid === "number", JSON.stringify(sent));
  return sent;
});
const sentRow = await first.lookupInvoice({ invoice: outside });
await check("lookup_invoice of a payment", async () => {
  must(sentRow.type === "outgoing" && sentRow.state === "settled", sentRow.state);
  must(sentRow.amount === 2_000_000 && preimageFits(sent.preimage, sentRow.payment_hash), sentRow.preimage);
  return sentRow.state;
});

await check("list_transactions", async () => {
  const { transactions, total_count } = await first.listTransactions({ limit: 10 });
  const seen = transactions.map((t) => `${t.type}:${t.amount}:${t.state}`);
  must(seen[0] === "outgoing:2000000:settled" && seen[1] === "incoming:1000000:settled", seen.join());
  must(total_count === 2, "total_count " + total_count);
  return seen;
});

await check("payment_received and payment_sent", async () => {
  await until("notifications", async () =>
    noted("payment_received", invoice.payment_hash) && noted("payment_sent", sentRow.payment_hash));
  const received = noted("payment_received", invoice.payment_hash).notification;
  must(received.type === "incoming" && received.amount === 1_000_000 && received.settled_at, JSON.stringify(received));
  return notes.length;
});

await check("pay own invoice (inside the ledger)", async () => {
  const own = await first.makeInvoice({ amount: 500_000, description: "own" });
  const paid = await first.payInvoice({ invoice: own.invoice });
  must(paid.fees_paid === 0 && preimageFits(paid.preimage, own.payment_hash), JSON.stringify(paid));
  await until("both notifications", async () =>
    noted("payment_received", own.payment_hash) && noted("payment_sent", own.payment_hash));
  const found = await first.lookupInvoice({ payment_hash: own.payment_hash });
  must(found.type === "outgoing" && found.state === "settled", found.state);
  return paid;
});

await check("get_budget with no limit is empty", async () => {
  const budget = await first.getBudget();
  must(Object.keys(budget).length === 0, JSON.stringify(budget));
  return budget;
});
await check("get_budget with a limit", async () => {
  const budget = await second.getBudget();
  must(budget.total_budget === Number(NWC_SECOND_LIMIT), JSON.stringify(budget));
  must(budget.used_budget === 0 && budget.renewal_period === "monthly", JSON.stringify(budget));
  must(budget.renews_at * 1000 > Date.now(), JSON.stringify(budget));
  return budget;
});
await check("get_balance stops at the limit", async () => {
  const { balance } = await second.getBalance();
  must(balance === Number(NWC_SECOND_LIMIT), "balance " + balance);
  return balance;
});
await check("second app sees none of it", async () => {
  const { transactions } = await second.listTransactions({ limit: 10, unpaid: true });
  must(transactions.length === 0, "shows " + transactions.length);
  return transactions;
});
await check("NOT_FOUND for another app's invoice", async () => {
  try {
    await second.lookupInvoice({ payment_hash: invoice.payment_hash });
  } catch (error) {
    must(error.code === "NOT_FOUND", error.code);
    return error.code;
  }
  throw new Error("not refused");
});

unsubscribe();
first.close();
second.close();
console.log(failed === 0 ? "ALL PASSED" : `${failed} FAILED`);
process.exit(failed === 0 ? 0 : 1);
