package com.tekcrux.objects

object ObjectExtended extends App {
   val actions = Map("open" -> DoNothingAction, "save" -> DoNothingAction)
   println(actions("open").description)
   actions("open").undo()
   actions("open").redo()
   actions("open") == actions("save")
}

abstract class UndoableAction(val description: String) {
    def undo(): Unit
    def redo(): Unit
}

object DoNothingAction extends UndoableAction("Do nothing") {
    override def undo() { 
        println ("undo() method of UndoableAction extended") 
    }
    override def redo() { 
        println ("redo() method of UndoableAction extended") 
    }
}

