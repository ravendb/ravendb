var debug_outputs = [];

function output(msg) {
	debug_outputs.push(msg.toString());
}

function LoadDocument(id) {
	var data = LoadDocumentInternal(id);
	return JSON.parse(data);
}