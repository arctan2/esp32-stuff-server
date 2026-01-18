fn test_pin_with_loop_back() {
    let config = esp_hal::Config::default().with_cpu_clock(CpuClock::max());
    let peripherals = esp_hal::init(config);

    let sck  = peripherals.GPIO13;
    let mosi = peripherals.GPIO21;
    let miso = peripherals.GPIO14;

    let mut data = [0xAA, 0xBB, 0xCC, 0xDD];

    println!("expected data = {:?}", &data);

    loop {
        match spi.transfer_in_place(&mut data) {
            Ok(_) => {
                if data == [0xAA, 0xBB, 0xCC, 0xDD] {
                    esp_println::println!("SUCCESS!");
                } else {
                    esp_println::println!("Received: {:?}", data);
                }
            }
            Err(_) => esp_println::println!("SPI Error"),
        }
        
        data = [0xAA, 0xBB, 0xCC, 0xDD];
        esp_hal::delay::Delay::new().delay_ms(1000u32);
    }
}

fn blink_led_infinite(mut led: Output<'static>) -> ! {
    let mut i = 0;
    loop {
        led.toggle();
        let delay_start = Instant::now();
        while delay_start.elapsed() < Duration::from_millis(500) {}
        println!("blink blink {i}");
        i += 1;
    }
}
