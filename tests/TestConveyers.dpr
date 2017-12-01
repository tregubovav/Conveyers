program TestConveyers;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Diagnostics,
  System.Generics.Collections,
  Conveyers in '..\units\Conveyers.pas';

var
  Log1, Log2, Log3, Log4: TList<string>;
  TEv: TCountdownEvent;

procedure AddLog(const Log: TList<string>; const Msg: string);
begin
  TMonitor.Enter(Log);
  try
    Log.Add(Msg);
  finally;
    TMonitor.Exit(Log);
  end;
end;

procedure PrintLog(const Log: TList<string>);
var
  Str: string;

begin
  for Str in Log do
    Writeln(Str);
  Writeln(Format('Items printed: %d', [Log.Count]));
end;

type
  TTestW2 = class(TCustomWorker<integer>)
    class var BID: integer;
    var FID: integer;
    procedure Execute(const Data: integer); override;
    constructor Create;
  end;

  TTestConv2 = class(TCustomConveyer<integer, TTestW2>)
    constructor Create(ADataQueueLen, AWorkerQueueLen: word;
      CreateSuspended: boolean = False);
    procedure NotifyLog(const Worker: TCustomWorker<integer>; var KeepWorker: boolean);
    procedure LogException(const Worker: TCustomWorker<integer>;
        const E: Exception; var KeepWorker: boolean);
  end;

  ETest = class(Exception);

procedure DoExeption;
begin
//  Randomize;
  if Random(2) = 0 then
    Raise ETest.Create('Test Exception');
end;

constructor TTestW2.Create;
begin
  inherited;
  TInterlocked.Increment(BID);
  FID:=BID;
end;

procedure TTestW2.Execute(const Data: integer);
var
  S: string;
  i: integer;

begin
  S:=Format('Worker Thread - %0:d, %0:d', [Data]);
  TThread.NameThreadForDebugging(S);
  for i := 0 to random(1000) do;
  AddLog(Log1, S);
//  DoExeption;
end;

constructor TTestConv2.Create(ADataQueueLen, AWorkerQueueLen: word;
  CreateSuspended: boolean = False);
begin
  Notify:=NotifyLog;
  HandleWorkerException:=LogException;
  inherited Create(ADataQueueLen, AWorkerQueueLen, CreateSuspended);
end;

procedure TTestConv2.NotifyLog(const Worker: TCustomWorker<integer>; var KeepWorker: boolean);
begin
//  KeepWorker:=(Random(8) > 0);
  case Worker.State of
    wsCreated:
    begin
      AddLog(Log3, Format('Worker #%d Created.',
        [(Worker as TTestW2).FID]));
//        DoExeption;
    end;
    wsReadyToExecute:
      begin
        AddLog(Log3, Format('Worker #%d is ready to Execute',
          [(Worker as TTestW2).FID]));
//        DoExeption;
      end;
    wsAfterExecute:
      begin
        AddLog(Log3, Format('Worker #%d completed Execution',
          [(Worker as TTestW2).FID]));
//        DoExeption;
      end;

    wsReadyToDestroy:
      begin
        AddLog(Log3, Format('Worker #%d is ready to Destroy',
          [(Worker as TTestW2).FID]));
        AddLog(Log4, Format('Worker #%d executed %d times.',
          [(Worker as TTestW2).FID, Worker.RunCount]));
//        DoExeption;
      end;
  end;
end;

procedure TTestConv2.LogException(const Worker: TCustomWorker<integer>;
        const E: Exception; var KeepWorker: boolean);
begin
  AddLog(Log3, Format('Exception "%s" raised in Worker #%d. Worker executed %d times.',
    [E.Message, (Worker as TTestW2).FID, Worker.RunCount]));
  Randomize;
  KeepWorker:=False;//(Random(8) = 0);
end;


const
  cPushItems = 1024;

var
  Conv2: TTestConv2;
  Timer: TStopwatch;

begin
  try
    { TODO -oUser -cConsole Main : Insert code here }
    Log1:=TList<string>.Create;
    Log2:=TList<string>.Create;
    Log3:=TList<string>.Create;
    Log4:=TList<string>.Create;
    Conv2:=TTestConv2.Create(128,4, True);
    Writeln(^M^J'Jobs started.'^M^J'Please wait...');
    TEv:= TCountdownEvent.Create(4);
    try
      Timer:=TStopwatch.StartNew;
      Conv2.Start;
      with TThread.CreateAnonymousThread(
        procedure
        var
          i: integer;

        begin
          TThread.NameThreadForDebugging('Odd Positive');
          i:=1;
          repeat
            Conv2.PushData(i);
            AddLog(Log2, Format('Odd Positive added, %d', [i]));
            Inc(i, 2);
            //Sleep(Random(100));
          until i > cPushItems;
          TEv.Signal;
        end
      ) do
        Start;

      with TThread.CreateAnonymousThread(
        procedure
        var
          i: integer;

        begin
          TThread.NameThreadForDebugging('Even Positive');
          i:=0;
          repeat
            Conv2.PushData(i);
            AddLog(Log2, Format('Even Positive added, %d', [i]));
            Inc(i, 2);
            Sleep(Random(100));
          until i > cPushItems;
          TEv.Signal;
        end
      ) do
        Start;

      with TThread.CreateAnonymousThread(
        procedure
        var
          i: integer;

        begin
          TThread.NameThreadForDebugging('Odd negative');
          i:=-1;
          repeat
            Conv2.PushData(i);
            AddLog(Log2, Format('Odd Negative added, %d', [i]));
            Dec(i, 2);
            Sleep(Random(100));
          until i < cPushItems*-1;
          TEv.Signal;
        end
      ) do
        Start;

      with TThread.CreateAnonymousThread(
        procedure
        var
          i: integer;

        begin
          TThread.NameThreadForDebugging('Even Negative');
          i:=-2;
          repeat
            Conv2.PushData(i);
            AddLog(Log2, Format('Even Negative added, %d', [i]));
            Dec(i, 2);
            Sleep(Random(100));
          until i < (cPushItems-1)*-1;
          TEv.Signal;
        end
      ) do
        Start;

      TEv.WaitFor;
      Writeln(^M^J'Data pushed:');
      PrintLog(Log2);
      Conv2.WaitForTerminate;
      Writeln(^M^J'Results:');
      Writeln('Name, ID');
      PrintLog(Log1);
      Log1.Clear;
    finally
      Conv2.Free;
    end;
    Writeln('Elasped time: ', Timer.ElapsedMilliseconds, 'ms.');
    Writeln('Done!');
    Writeln('---'^M^J'Debug:');
    PrintLog(Log3);
    Writeln('---'^M^J'Termination stats:');
    PrintLog(Log4);
    Log4.Free;
    Log3.Free;
    Log2.Free;
    Log1.Free;


  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
  Writeln('Press enter to exit.');
  Readln;
end.
