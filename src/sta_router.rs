use picoserve::routing::get;

static STA_WEBPAGE: &str = include_str!("./sta_client/index.html");

pub fn sta_router() -> picoserve::Router<impl picoserve::routing::PathRouter> {
    picoserve::Router::new()
        .route("/", get(|| async {
            picoserve::response::Response::ok(STA_WEBPAGE).with_header("Content-Type", "text/html")
        }))
}
