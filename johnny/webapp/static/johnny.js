// Common JavaScript code.
//
//  Copyright (C) 2021  Martin Blais
//  License: GNU GPLv2

function CreateChainsTable(id, extra_config) {
    // Create the table instance.
    var config = {
        pageLength: 200,
        select: 'multi+shift',
        fixedHeader: true,
        colReorder: true,
        columnDefs: [
            {targets: ['init', 'pnl_chain', 'net_liq', 'commissions', 'fees'],
             className: 'dt-body-right'},
        ],

        // This gets called once when the table is drawn.
        footerCallback: function (row, data, start, end, display) {
            var api = this.api()
            UpdateFooter(api);
        }
    };
    if (extra_config != null) {
        config = Object.assign(config, extra_config)
    }
    var table = $(id).DataTable(config);

    // Emphasize some columns of the table.
    $(table.column(':contains(pnl_chain)').nodes()).addClass('emph-column');

    table.on('select.dt', function () {
        UpdateFooter(table);
    });
    table.on('deselect.dt', function () {
        UpdateFooter(table);
    });

    InstallDataTableFocus(table);

    return table;
}

// Bind SLASH to focus on the search box of the DataTable instance from
// anywhere.
function InstallDataTableFocus(table) {
    // Install a handler for focusing on a key pressa.
    $(document).keypress(function(ev) {
        var tdiv = table.table().container()
        var search_input = $(tdiv).find(".dataTables_filter input");
        // console.log(search_input);
        if (ev.which == 47 && ev.key == '/')  {
            if (!search_input.is(":focus")) {
                event.preventDefault();
                search_input.focus();
            }
        }
    });
}

function SumFloat(a, b) {
    return (parseFloat(a) + parseFloat(b)).toFixed(2);
}
function SumInteger(a, b) {
    return parseInt(a) + parseInt(b);
}

// Update the footer sums on a selection change.
function UpdateFooter(table) {
    const columns = {
        init: SumFloat,
        init_legs: SumInteger,
        pnl_chain: SumFloat,
        net_liq: SumFloat,
        commissions: SumFloat,
        fees: SumFloat,
    }
    $.each(columns, function(colname, reducer) {
        var contains = ':contains(' + colname + ')';
        var column = table.column(contains);
        var data = table.cells('.selected', contains).data();
        if (data.length == 0) {
            data = table.cells({search: 'applied'}, contains).data();
            if (data.length == 0) {
                $(column.footer()).html('N/A');
                return;
            }
        }
        var sum = data.reduce(reducer);
        $(column.footer()).html(sum);
    });
}
