
export default function initializeToggleButtons() {
    $('.btn-toggle').click(function (e) {
        var target = $(this).attr('data-target');
        var targetClass = $(this).attr('data-class');
        $(target).toggleClass(targetClass);
    });
}
