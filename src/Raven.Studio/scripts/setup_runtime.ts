import "@testing-library/jest-dom";
import { initI18n } from "common/i18n/i18n";
import { commonInit } from "components/common/shell/setup";

initI18n({ onMissingKey: "throw" });
commonInit();
