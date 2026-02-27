pub trait StateMachine {
    type Command;
    type Response;

    fn apply(&mut self, command: Self::Command) -> Self::Response;
}
