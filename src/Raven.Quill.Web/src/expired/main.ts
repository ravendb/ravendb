import "./expired.css";
import quillLogo from "@/components/brand/quill-logo.svg?raw";
import interLatin from "@fontsource-variable/inter/files/inter-latin-wght-normal.woff2?inline";

const inter = new FontFace("Inter Variable", `url(${interLatin})`, { weight: "100 900", display: "swap" });
document.fonts.add(inter);
void inter.load();

// Inline svg rather than an <img>, so the logo keeps following the theme via currentColor.
document.getElementById("logo")?.insertAdjacentHTML("afterbegin", quillLogo);
