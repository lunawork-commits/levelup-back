'use strict';

(function () {
    var POS_IIKO = 'iiko';
    var POS_DOOGLYS = 'dooglys';

    function getFieldset(className) {
        return document.querySelector('fieldset.' + className);
    }

    function updateSections(posType) {
        var iiko = getFieldset('pos-iiko');
        var dooglys = getFieldset('pos-dooglys');

        if (iiko) iiko.style.display = posType === POS_IIKO ? '' : 'none';
        if (dooglys) dooglys.style.display = posType === POS_DOOGLYS ? '' : 'none';
    }

    function init() {
        var select = document.getElementById('id_pos_type');
        if (!select) return;

        // Применить сразу при загрузке страницы
        updateSections(select.value);

        select.addEventListener('change', function () {
            updateSections(this.value);
        });
    }

    document.addEventListener('DOMContentLoaded', init);
})();
