use esp_println::println;
use core::net::Ipv4Addr;
use embassy_net::{Stack};
use leasehund::{DhcpServer, DhcpConfigBuilder, DhcpConfig};

#[embassy_executor::task(pool_size = 4)]
pub async fn dhcp_server_task(stack: Stack<'static>) {
    let config: DhcpConfig<4> = DhcpConfigBuilder::new()
        .server_ip(Ipv4Addr::new(10, 0, 1, 1))
        .subnet_mask(Ipv4Addr::new(255, 255, 255, 0))
        .router(Ipv4Addr::new(10, 0, 1, 1))
        .add_dns_server(Ipv4Addr::new(1, 1, 1, 1))
        .ip_pool(
            Ipv4Addr::new(10, 0, 1, 100),
            Ipv4Addr::new(10, 0, 1, 150)
        )
        .lease_time(7200)
        .build();

    let mut dhcp_server: DhcpServer<32, 4> = DhcpServer::with_config(config);
    
    println!("DHCP Server started on 10.0.1.1");
    dhcp_server.run(stack).await;
}

