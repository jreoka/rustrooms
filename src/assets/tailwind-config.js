        // Neutral ramp driven by the --c-* CSS variables in app.css,
        // so every zinc/slate/gray/neutral utility follows the
        // active theme (dark by default, light via data-theme).
        const rrNeutral = {
            50: 'rgb(var(--c-surface) / <alpha-value>)',
            100: 'rgb(var(--c-surface) / <alpha-value>)',
            200: 'rgb(var(--c-container) / <alpha-value>)',
            300: 'rgb(var(--c-on-surface) / <alpha-value>)',
            400: 'rgb(var(--c-on-surface-variant) / <alpha-value>)',
            500: 'rgb(var(--c-on-surface-variant) / <alpha-value>)',
            600: 'rgb(var(--c-on-surface-variant) / <alpha-value>)',
            700: 'rgb(var(--c-outline) / <alpha-value>)',
            800: 'rgb(var(--c-highest) / <alpha-value>)',
            900: 'rgb(var(--c-high) / <alpha-value>)',
            950: 'rgb(var(--c-low) / <alpha-value>)',
        };
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
                        slate: rrNeutral,
                        zinc: rrNeutral,
                        gray: rrNeutral,
                        neutral: rrNeutral,
                        indigo: {
                            50: '#C5C9FF', 100: '#C5C9FF', 200: '#C5C9FF', 300: '#C5C9FF',
                            400: '#7178F4', 500: '#575EEA', 600: '#575EEA', 700: '#2E3266',
                            800: '#2E3266', 900: '#2E3266', 950: '#2E3266',
                        },
                        // Stray blue utilities (e.g. text-blue-400 status
                        // icons) land on the same single accent.
                        blue: {
                            50: '#C5C9FF', 100: '#C5C9FF', 200: '#C5C9FF', 300: '#C5C9FF',
                            400: '#7178F4', 500: '#575EEA', 600: '#575EEA', 700: '#2E3266',
                            800: '#2E3266', 900: '#2E3266', 950: '#2E3266',
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
