self.addEventListener("push", (event) => {
    let payload = {};
    try {
        payload = event.data ? event.data.json() : {};
    } catch (e) {
        payload = { title: "Fresh Grocery", message: "You have a new update." };
    }

    const title = payload.title || "Fresh Grocery";
    const options = {
        body: payload.message || "You have a new update.",
        data: { order_id: payload.order_id || null, url: "/" },
    };

    event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
    event.notification.close();
    const targetUrl = (event.notification.data && event.notification.data.url) || "/";
    event.waitUntil(clients.openWindow(targetUrl));
});
