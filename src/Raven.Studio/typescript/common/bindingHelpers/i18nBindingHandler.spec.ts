import i18nBindingHandler = require("common/bindingHelpers/i18nBindingHandler");
import i18nModule = require("common/i18n/i18n");

const { i18n, initI18n } = i18nModule;

describe("i18nBindingHandler", () => {
    beforeAll(() => {
        initI18n();
        i18nBindingHandler.install();
        i18n.addResourceBundle("en", "spec", {
            heading_new: "New thing",
            heading_edit: "Edit thing",
            greeting: "Hello {{name}}",
        });
        i18n.addResourceBundle("pl", "spec", {
            heading_new: "Nowa rzecz",
            heading_edit: "Edycja rzeczy",
            greeting: "Cześć {{name}}",
        });
    });

    afterEach(async () => {
        await i18n.changeLanguage("en");
    });

    function bind(html: string, viewModel: object = {}): HTMLElement {
        const container = document.createElement("div");
        container.innerHTML = html;
        ko.applyBindings(viewModel, container);
        return container.firstElementChild as HTMLElement;
    }

    it("sets text content from a namespaced key", () => {
        const element = bind(`<span data-bind="i18n: 'common:save'"></span>`);
        expect(element.textContent).toBe("Save");
    });

    it("supports key object with context", () => {
        const element = bind(`<h3 data-bind="i18n: { key: 'spec:heading', options: { context: 'new' } }"></h3>`);
        expect(element.textContent).toBe("New thing");
    });

    it("interpolates options", () => {
        const element = bind(`<span data-bind="i18n: { key: 'spec:greeting', options: { name: 'Ada' } }"></span>`);
        expect(element.textContent).toBe("Hello Ada");
    });

    it("unwraps observables in options and reacts to their change", () => {
        const mode = ko.observable("new");
        const element = bind(`<h3 data-bind="i18n: { key: 'spec:heading', options: { context: mode } }"></h3>`, {
            mode,
        });
        expect(element.textContent).toBe("New thing");

        mode("edit");
        expect(element.textContent).toBe("Edit thing");
    });

    it("sets attributes", () => {
        const element = bind(
            `<input data-bind="i18nAttr: { title: 'common:cancel', placeholder: { key: 'spec:greeting', options: { name: 'Bob' } } }">`
        );
        expect(element.getAttribute("title")).toBe("Cancel");
        expect(element.getAttribute("placeholder")).toBe("Hello Bob");
    });

    it("re-evaluates text and attributes on language change", async () => {
        const text = bind(`<span data-bind="i18n: 'common:save'"></span>`);
        const attr = bind(`<button data-bind="i18nAttr: { title: 'common:cancel' }"></button>`);

        await i18n.changeLanguage("pl");

        expect(text.textContent).toBe("Zapisz");
        expect(attr.getAttribute("title")).toBe("Anuluj");
    });

    it("never writes HTML", () => {
        i18n.addResourceBundle("en", "spec", { html: "<b>bold</b>" }, true, true);
        const element = bind(`<span data-bind="i18n: 'spec:html'"></span>`);
        expect(element.textContent).toBe("<b>bold</b>");
        expect(element.children.length).toBe(0);
    });
});
