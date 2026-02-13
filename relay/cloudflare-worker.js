export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname !== "/run-now") {
      return new Response("Not found", { status: 404 });
    }

    const key = url.searchParams.get("key");
    if (!key || key !== env.RUN_NOW_KEY) {
      return new Response("Unauthorized", { status: 401 });
    }

    const forceNotify = url.searchParams.get("force_notify") === "true";
    const debugLog = url.searchParams.get("debug_log") === "true";

    const dispatchResp = await fetch(
      `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}/dispatches`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.GH_TOKEN}`,
          Accept: "application/vnd.github+json",
          "Content-Type": "application/json",
          "User-Agent": "deddie-run-now-relay",
        },
        body: JSON.stringify({
          event_type: "run_now",
          client_payload: {
            force_notify: forceNotify ? "true" : "false",
            debug_log: debugLog ? "true" : "false",
          },
        }),
      },
    );

    if (!dispatchResp.ok) {
      const errorBody = await dispatchResp.text();
      return new Response(
        `Dispatch failed (${dispatchResp.status}): ${errorBody}`,
        { status: 500 },
      );
    }

    return Response.redirect(
      `https://github.com/${env.GH_OWNER}/${env.GH_REPO}/actions/workflows/monitor.yml`,
      302,
    );
  },
};
