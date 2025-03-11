import { resetAllMocks } from "@storybook/test";
import { useState } from "react";
import { createStoreConfiguration } from "components/store";
import { setEffectiveTestStore } from "components/storeCompat";
import { Provider } from "react-redux";

export const StoreDecorator = (Story) => {
    resetAllMocks();

    const [store] = useState(() => {
        const storeConfiguration = createStoreConfiguration();
        setEffectiveTestStore(storeConfiguration);
        return storeConfiguration;
    });

    return (
        <Provider store={store}>
            <Story />
        </Provider>
    );
};
