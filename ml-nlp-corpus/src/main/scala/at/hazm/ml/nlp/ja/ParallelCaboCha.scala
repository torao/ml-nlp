package at.hazm.ml.nlp.ja

import at.hazm.core.util.ResourcePool
import at.hazm.ml.nlp.ja.CaboCha.Token
import at.hazm.ml.nlp.model.{RelativeDocument, RelativeSentence}

import scala.concurrent.{ExecutionContext, Future}

/**
  * CaboCha の外部プロセスを並列で行うクラスです。
  *
  * @param parallel 並列実行数
  * @param cmd      CaboCha コマンド
  */
class ParallelCaboCha(parallel:Int, cmd:String = "cabocha") extends AutoCloseable {

  private[this] val pool = new ResourcePool[CaboCha](parallel, new ResourcePool.Factory[CaboCha] {
    override def acquire():CaboCha = new CaboCha(cmd)

    override def dispose(resource:CaboCha):Unit = resource.close()
  })

  /**
    * 指定されたドキュメントを解析しパラグラフを作成します。並列実行数を超過している場合、呼び出しはブロックされます。
    *
    * @param id   ドキュメントID
    * @param text ドキュメントのテキスト
    * @return 解析結果のパラグラフ
    */
  def tokenize(id:Int, text:String)(implicit _context:ExecutionContext):Future[RelativeDocument[Token]] = {
    pool.acquireAndRun { cabocha =>
      cabocha.tokenize(id, text)
    }
  }

  /**
    * 起動しているコマンドをすべて終了します。
    */
  override def close():Unit = pool.close()
}
