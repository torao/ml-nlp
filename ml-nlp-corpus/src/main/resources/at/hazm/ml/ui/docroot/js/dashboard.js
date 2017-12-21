(function(){

  /**
   * 文書の自動生成 API
  */
  function generateFollowingSentences(text, callback){
    $.ajax({
      type: "POST",
      url: "/api/1.0/predict_following_sentences",
      data: JSON.stringify({ text: text }),
      contentType: "application/json",
      dateType: "JSON",
      success: function(json){
        callback(json["text"]);
      }
    });
  }

  $(function(){
    var MENUS = ["dashboard", "gen_sentence"];

    // URL ハッシュに付けられたメニューを取得
    var current = window.location.hash;
    if(current == null || current.length == 0 || current == "#"){
      current = MENUS[0];
    } else if(current.startsWith("#")){
      current = current.substring(1);
    }

    // メニューが選択されたときの画面切り替え処理を設定
    MENUS.forEach(function(menu){
      $("#div_" + menu).show(menu == current);    // 表示制御
      $("#menu_" + menu).click(function(){
        MENUS.forEach(function(menu){ $("#div_" + menu).hide(); });
        $("#div_" + menu).show();
        window.location.hash = menu;
        return false;
      });
    });
    $("#menu_" + current).click();

    // 文書生成 API の実行
    var sentenceInputTimer = null;
    $("#gen_sentence_input").keypress(function(){
      if(sentenceInputTimer != null) clearTimeout(sentenceInputTimer);
      sentenceInputTimer = setTimeout(function(){
        sentenceInputTimer = null;
        var text = $("#gen_sentence_input").val();
        if(text.trim().length > 0){
          generateFollowingSentences(text.trim(), function(newSentence){
            $("#gen_sentence_output").val(newSentence);
          });
        }
      }, 800);
    });
  });
})();