// Set a fixed width for the comment column, and wrap text.

// Wrap text in columnDefs.

             // width: '20%',
             render: function (data, type, full, meta) {
                 let instaText = (data != null && data.length > 25) ? data.substr(0, 25) : data == null ? "" : data;
                 return '<div class="text-overflow" title='+'"'+ data +'"' +'>' + instaText + '</div>';
             },
