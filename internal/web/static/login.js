// Login with a NIP-07 browser extension: it signs a NIP-98 event over the login
// URL, which carries the server's one-time challenge.
"use strict";
(function () {
  const button = document.getElementById("login");
  const message = document.getElementById("message");
  if (!button) {
    return;
  }
  button.addEventListener("click", async function () {
    message.textContent = "";
    if (!window.nostr) {
      message.textContent = "No Nostr extension found. Install a NIP-07 extension, such as Alby or nos2x.";
      return;
    }
    const url = window.location.origin + "/login?challenge=" + encodeURIComponent(button.dataset.challenge);
    button.disabled = true;
    try {
      const event = await window.nostr.signEvent({
        kind: 27235,
        created_at: Math.floor(Date.now() / 1000),
        tags: [["u", url], ["method", "POST"]],
        content: "",
      });
      const response = await fetch(url, {
        method: "POST",
        credentials: "same-origin",
        headers: { Authorization: "Nostr " + btoa(JSON.stringify(event)) },
      });
      if (response.ok) {
        window.location.assign("/");
        return;
      }
      message.textContent = "Login refused. Reload the page to try again.";
    } catch (error) {
      message.textContent = "Signing failed: " + error;
    }
  });
})();
