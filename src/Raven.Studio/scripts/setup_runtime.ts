import "@testing-library/jest-dom";
import { initI18n } from "common/i18n/i18n";
import { commonInit } from "components/common/shell/setup";
import { setEffectiveTestStore } from "components/storeCompat";
import { createTestStore } from "test/createTestStore";

initI18n({ onMissingKey: "throw" });
setEffectiveTestStore(createTestStore());
commonInit();
