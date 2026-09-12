import ace from "ace-builds/src-noconflict/ace";
import cssWorkerUrl from "ace-builds/src-noconflict/worker-css?url";
import htmlWorkerUrl from "ace-builds/src-noconflict/worker-html?url";
import javascriptWorkerUrl from "ace-builds/src-noconflict/worker-javascript?url";
import jsonWorkerUrl from "ace-builds/src-noconflict/worker-json?url";
import xmlWorkerUrl from "ace-builds/src-noconflict/worker-xml?url";
import yamlWorkerUrl from "ace-builds/src-noconflict/worker-yaml?url";

// Ace loads validation workers by guessing a script path relative to the current page URL,
// which 404s under Vite. Point every worker used by the imported modes at the URL Vite
// serves for the bundled file (markdown/html embed the javascript/css/xml workers).
ace.config.setModuleUrl("ace/mode/css_worker", cssWorkerUrl);
ace.config.setModuleUrl("ace/mode/html_worker", htmlWorkerUrl);
ace.config.setModuleUrl("ace/mode/javascript_worker", javascriptWorkerUrl);
ace.config.setModuleUrl("ace/mode/json_worker", jsonWorkerUrl);
ace.config.setModuleUrl("ace/mode/xml_worker", xmlWorkerUrl);
ace.config.setModuleUrl("ace/mode/yaml_worker", yamlWorkerUrl);
