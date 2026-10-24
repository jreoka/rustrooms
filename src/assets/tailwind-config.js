        tailwind.config = {
            future: {
                hoverOnlyWhenSupported: true,
            },
            theme: {
                extend: {
                    colors: {
                        // Facelift tokens — utilities like bg-zinc-800,
                        // text-zinc-400, border-zinc-700 resolve to the
                        // exact hexes of the app.css palette: neutral zinc
                        // surfaces, hairline borders, one indigo accent.
                        slate: {
                            50: '#0B0C0E', 100: '#0B0C0E', 200: '#17181C', 300: '#ECEEF1',
                            400: '#A7AAB3', 500: '#A7AAB3', 600: '#A7AAB3', 700: '#3A3B43',
                            800: '#26272E', 900: '#1E1F24', 950: '#121316',
                        },
                        zinc: {
                            50: '#0B0C0E', 100: '#0B0C0E', 200: '#17181C', 300: '#ECEEF1',
                            400: '#A7AAB3', 500: '#A7AAB3', 600: '#A7AAB3', 700: '#3A3B43',
                            800: '#26272E', 900: '#1E1F24', 950: '#121316',
                        },
                        gray: {
                            50: '#0B0C0E', 100: '#0B0C0E', 200: '#17181C', 300: '#ECEEF1',
                            400: '#A7AAB3', 500: '#A7AAB3', 600: '#A7AAB3', 700: '#3A3B43',
                            800: '#26272E', 900: '#1E1F24', 950: '#121316',
                        },
                        neutral: {
                            50: '#0B0C0E', 100: '#0B0C0E', 200: '#17181C', 300: '#ECEEF1',
                            400: '#A7AAB3', 500: '#A7AAB3', 600: '#A7AAB3', 700: '#3A3B43',
                            800: '#26272E', 900: '#1E1F24', 950: '#121316',
                        },
                        indigo: {
                            50: '#C9CFF5', 100: '#C9CFF5', 200: '#C9CFF5', 300: '#C9CFF5',
                            400: '#7180DC', 500: '#5E6AD2', 600: '#5E6AD2', 700: '#2C3260',
                            800: '#2C3260', 900: '#2C3260', 950: '#2C3260',
                        },
                        // Stray blue utilities (e.g. text-blue-400 status
                        // icons) land on the same single accent.
                        blue: {
                            50: '#C9CFF5', 100: '#C9CFF5', 200: '#C9CFF5', 300: '#C9CFF5',
                            400: '#7180DC', 500: '#5E6AD2', 600: '#5E6AD2', 700: '#2C3260',
                            800: '#2C3260', 900: '#2C3260', 950: '#2C3260',
                        },
                        red: {
                            500: '#E5484D', 600: '#D63A3F', 700: '#C93A3E', 800: '#4C1D1F',
                            900: '#4C1D1F', 950: '#331417',
                        },
                        green: {
                            500: '#3BA55C', 600: '#3BA55C', 700: '#359550', 800: '#2E8B4A',
                            900: '#2E8B4A', 950: '#2E8B4A',
                        },
                        amber: {
                            500: '#E3B341', 600: '#E3B341', 700: '#E3B341', 800: '#E3B341',
                            900: '#E3B341', 950: '#E3B341',
                        }
                    }
                }
            }
        }
