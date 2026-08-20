namespace Raven.Quill.Hosting;

internal static class ExpiryNotice
{
    internal const string Page = $"""
        <!doctype html>
        <html lang="en">
        <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Quill build expired</title>
        <style>{Styles}</style>
        <script>{ThemeScript}</script>
        </head>
        <body>
        <main class="q-screen">
        <div class="q-wash" aria-hidden="true"></div>
        <div class="q-col">
        <div class="q-brand">{Logo}</div>
        <div class="q-alert" role="alert">
        <div class="q-title">This Quill build has expired</div>
        <div class="q-desc">
        <p>Your Quill needs an update. Pull the latest image:</p>
        <div class="q-cmd">
        <pre id="q-command">docker pull ravendb/quill:latest</pre>
        <button class="q-copy" type="button">Copy</button>
        </div>
        <p>After a successful update, run the Docker container again.</p>
        </div>
        </div>
        </div>
        </main>
        <script>{CopyScript}</script>
        </body>
        </html>

        """;

    private const string ThemeScript =
        """
        var t = localStorage.getItem("theme");
        if (t === "dark" || ((t === "system" || t === null) && matchMedia("(prefers-color-scheme: dark)").matches))
            document.documentElement.classList.add("dark");
        """;

       private const string CopyScript =
        """
        var pre = document.getElementById("q-command");
        var btn = document.querySelector(".q-copy");
        var reset;
        function flash(label) {
            btn.textContent = label;
            clearTimeout(reset);
            reset = setTimeout(function () { btn.textContent = "Copy"; }, 2000);
        }
        btn.addEventListener("click", function () {
            var range = document.createRange();
            range.selectNodeContents(pre);
            var selection = getSelection();
            selection.removeAllRanges();
            selection.addRange(range);

            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(pre.textContent).then(
                    function () { selection.removeAllRanges(); flash("Copied"); },
                    function () { flash("Press Ctrl+C"); });
                return;
            }

            flash("Press Ctrl+C");
        });
        """;

      private const string Styles =
        """
        :root{
        --tint-h:31.73;--tint-c:0;
        --brand-500:#ff775f;
        --radius:0.625rem;
        --background:oklch(0.985 calc(0.005 * var(--tint-c)) var(--tint-h));
        --foreground:oklch(0.18 calc(0.006 * var(--tint-c)) var(--tint-h));
        --card:oklch(1 0 0);
        --muted:oklch(0.955 calc(0.006 * var(--tint-c)) var(--tint-h));
        --muted-foreground:oklch(0.45 calc(0.015 * var(--tint-c)) var(--tint-h));
        --border:oklch(0.3 calc(0.04 * var(--tint-c)) var(--tint-h) / 12%);
        --destructive:oklch(0.5 0.21 14);
        --ring:oklch(0.565 0.1708 31.73);
        --font-sans:"Inter Variable",ui-sans-serif,system-ui,sans-serif;
        --font-mono:"Geist Mono Variable",ui-monospace,"SFMono-Regular",Menlo,monospace;
        }
        html.dark{
        --background:oklch(0.145 calc(0.008 * var(--tint-c)) var(--tint-h));
        --foreground:oklch(0.985 0 0);
        --card:oklch(0.205 calc(0.01 * var(--tint-c)) var(--tint-h));
        --muted:oklch(0.269 calc(0.012 * var(--tint-c)) var(--tint-h));
        --muted-foreground:oklch(0.708 calc(0.012 * var(--tint-c)) var(--tint-h));
        --border:oklch(1 0 0 / 10%);
        --destructive:oklch(0.7 0.2 14);
        --ring:#ff775f;
        }
        html,body{margin:0}
        .q-screen{position:relative;display:flex;min-height:100svh;flex-direction:column;align-items:center;
        justify-content:center;overflow:hidden;padding:2.5rem 1rem;background:var(--background);
        color:var(--foreground);font-family:var(--font-sans);-webkit-font-smoothing:antialiased}
        .q-wash{position:absolute;inset:0;pointer-events:none;
        background:radial-gradient(70% 55% at 50% -10%, color-mix(in oklch, var(--brand-500) 16%, transparent), transparent 70%)}
        .q-col{position:relative;z-index:1;display:flex;width:100%;max-width:28rem;flex-direction:column;align-items:center}
        .q-brand{margin-bottom:2rem;color:var(--foreground)}
        .q-brand svg{display:block;width:120px;height:auto}
        .q-alert{display:grid;gap:.125rem;width:100%;padding:.5rem .625rem;text-align:left;font-size:.875rem;
        background:var(--card);border:1px solid var(--border);border-radius:var(--radius)}
        .q-title{font-weight:500;color:var(--destructive)}
        .q-desc{color:var(--muted-foreground);line-height:1.55}
        .q-desc p{margin:0}
        .q-desc p+p{margin-top:.5rem}
        .q-desc code{font-family:var(--font-mono);font-size:.9em}
        .q-cmd{position:relative;margin:.5rem 0}
        .q-cmd pre{margin:0;padding:.5rem 5.25rem .5rem .625rem;overflow-x:auto;color:var(--foreground);
        background:var(--muted);border-radius:calc(var(--radius) - 2px);
        font-family:var(--font-mono);font-size:.8125rem}
        .q-copy{position:absolute;top:.3125rem;right:.3125rem;display:inline-flex;align-items:center;
        justify-content:center;height:1.75rem;padding:0 .625rem;border:1px solid transparent;
        border-radius:min(calc(var(--radius) - 2px),12px);background:transparent;color:var(--muted-foreground);
        font-family:var(--font-sans);font-size:.8rem;font-weight:500;line-height:1;white-space:nowrap;
        cursor:pointer;transition:background-color .15s,color .15s}
        .q-copy:hover{background:var(--card);color:var(--foreground)}
        .q-copy:focus-visible{outline:3px solid color-mix(in oklab, var(--ring) 50%, transparent);
        outline-offset:2px}
        """;

    private const string Logo =
        """
        <svg role="img" aria-label="Quill by RavenDB" viewBox="0 0 145 58" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M95.59 21.2871V57.0732H88.795V51.5634H88.6438C87.7899 53.4728 86.3005 55.0182 84.1923 56.1997C82.0757 57.3812 79.6371 57.9803 76.8598 57.9803C75.6867 57.9803 74.5556 57.86 73.4693 57.622C72.9262 57.5044 72.397 57.3588 71.8791 57.1824C70.3196 56.6533 68.9533 55.8358 67.7719 54.7299C67.3603 54.3463 66.9823 53.9263 66.6324 53.4728C65.9912 52.6497 65.4509 51.7006 65.0029 50.6283C64.9833 50.5723 64.9609 50.5191 64.9329 50.4631C64.233 48.7244 63.8774 46.6778 63.8774 44.3065V21.2871H70.678V42.4251C70.678 44.0881 70.9048 45.502 71.3499 46.6918C71.8119 47.8733 72.4082 48.828 73.1641 49.556C73.8137 50.1887 74.55 50.6703 75.3647 50.995C75.4963 51.0482 75.6307 51.0986 75.7679 51.1434C76.7506 51.4738 77.7669 51.6306 78.8252 51.6306C80.2335 51.6306 81.5465 51.4038 82.756 50.9586C83.9655 50.5051 85.0238 49.7827 85.9225 48.8084C86.8296 47.8173 87.5379 46.5743 88.0391 45.0624C88.5402 43.5589 88.795 41.7727 88.795 39.7037V21.2871H95.59Z" fill="currentColor"/>
        <path d="M104.873 21.2929V57.0819H111.668V21.2929H104.873ZM111.785 4.95091C110.794 3.96821 109.627 3.47266 108.266 3.47266C106.905 3.47266 105.738 3.96821 104.758 4.95091C103.781 5.92522 103.285 7.1011 103.285 8.46177C103.285 9.82244 103.781 10.9899 104.758 11.9726C105.738 12.9469 106.914 13.4425 108.266 13.4425C109.618 13.4425 110.794 12.9469 111.785 11.9726C112.759 10.9899 113.255 9.81404 113.255 8.46177C113.255 7.1095 112.759 5.92522 111.785 4.95091Z" fill="currentColor"/>
        <path d="M121.488 0V57.0781H128.274V0H121.488Z" fill="currentColor"/>
        <path d="M138.129 0V57.0781H144.916V0H138.129Z" fill="currentColor"/>
        <path d="M54.9895 47.5146C53.682 46.5543 52.2541 45.7563 50.7143 45.1572C48.9084 44.4377 46.957 43.9897 44.9244 43.8665C44.5325 43.8385 44.1349 43.8245 43.7346 43.8245C43.4322 43.8245 43.1438 43.8245 42.8498 43.8525C42.7238 43.6621 42.5951 43.4746 42.4579 43.2954C42.4103 43.2198 42.3543 43.1442 42.2983 43.077C41.9987 42.6766 41.6852 42.2903 41.3548 41.9151C41.0328 41.5399 40.6912 41.1788 40.3385 40.8372C40.3385 40.8316 40.3357 40.8288 40.3301 40.826C39.7113 40.2157 39.0478 39.6473 38.3479 39.1266C35.1198 36.7076 31.9281 35.2657 27.5717 35.2657C21.6223 35.2657 16.3616 38.7766 12.7976 42.9874C10.0566 39.7201 8.04362 34.9018 8.04362 30.5398C8.04362 19.814 16.7368 11.1181 27.4681 11.1181C37.1132 11.1181 45.1148 18.1454 46.6295 27.3593C46.8003 28.3952 46.8898 29.4563 46.8898 30.5398C46.8898 34.7282 45.4928 38.5078 43.2278 41.6743C43.2866 41.7415 43.337 41.7975 43.3818 41.8479C43.5862 42.0775 43.6814 42.1615 43.8997 42.4443C47.2314 42.3491 49.7316 43.2478 51.4506 44.0177C51.6074 43.7377 51.7614 43.4522 51.9098 43.1638C53.8416 39.3925 54.9279 35.0586 54.9279 30.5398C54.9279 29.4647 54.8663 28.4008 54.7431 27.3593C53.1696 13.6882 41.5536 3.08008 27.4681 3.08008C12.3048 3.08008 0 15.3681 0 30.5398C0 38.0683 3.01531 44.8828 7.92883 49.8383C12.8675 54.8218 19.5841 57.7812 26.6058 57.9883C29.3299 58.0695 32.0653 57.7644 34.7082 57.0924C36.5728 56.6165 38.3171 55.7766 39.9801 54.8246C41.6404 53.8727 43.2558 53.0104 45 52.2181C48.8553 50.4711 53.1108 50.9638 57.0585 51.9325C58.0132 52.1677 58.9567 52.4505 59.8806 52.7892C58.5955 50.7314 56.9325 48.9396 54.9923 47.5118L54.9895 47.5146ZM32.1381 47.467H31.9813C30.1391 47.2962 28.6496 45.8991 28.3724 44.0737L28.336 43.8301L28.2353 43.1862L35.3354 44.1997L36.2201 44.3257C35.8757 46.2715 34.1035 47.6321 32.1381 47.467Z" fill="currentColor"/>
        </svg>
        """;
}
