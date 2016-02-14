-module(zdoc).
-compile(export_all).

run() ->
    Base = "../",
    Ext = "erl",
    {ok, FS} = file:list_dir(Base),
    TS = [string:tokens(F, ".") || F <- FS],
    Files = [hd(T) || T <- TS, hd(tl(T)) == Ext],
    Paths = [Base ++ F ++ "." ++ Ext || F <- Files],
    edoc:files(Paths, [{dir, Base ++ "edoc"}, {private, true}]).
